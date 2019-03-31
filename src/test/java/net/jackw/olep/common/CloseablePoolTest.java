package net.jackw.olep.common;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CloseablePoolTest {
    @SuppressWarnings({"unchecked", "MustBeClosedChecker"})
    @Test(timeout = 500)
    public void testPoolUsesValuesFromSupplier() throws InterruptedException {
        Supplier<ProtectedResource> supplier = spy(Supplier.class);
        ProtectedResource[] protectedResources = new ProtectedResource[]{new ProtectedResource(), new ProtectedResource(), new ProtectedResource()};

        when(supplier.get()).thenReturn(protectedResources[0])
            .thenReturn(protectedResources[1])
            .thenReturn(protectedResources[2]);

        CloseablePool<ProtectedResource> pool = new CloseablePool<>(supplier, 3);

        verify(supplier, times(3)).get();

        assertThat(
            List.of(pool.acquire().get(), pool.acquire().get(), pool.acquire().get()),
            Matchers.containsInAnyOrder(protectedResources[0], protectedResources[1], protectedResources[2])
        );
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test(timeout = 500)
    public void testPoolBlocksWhenNoResourcesAvailable() throws InterruptedException {
        final ProtectedResource resource = new ProtectedResource();
        final CloseablePool<ProtectedResource> pool = new CloseablePool<>(() -> resource, 1);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean awoken = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                try (CloseablePool<ProtectedResource>.Resource _r = pool.acquire()) {
                    latch.countDown();
                    Thread.sleep(20);
                    awoken.set(true);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();

        assertSame(resource, pool.acquire().get());

        assertTrue(awoken.get());
    }

    @Test(timeout = 500)
    public void testReleasedResourcesAreReassigned() throws InterruptedException {
        ProtectedResource resource = new ProtectedResource();
        CloseablePool<ProtectedResource> pool = new CloseablePool<>(() -> resource, 1);

        try (CloseablePool<ProtectedResource>.Resource r = pool.acquire()) {
            assertSame(resource, r.get());
        }

        try (CloseablePool<ProtectedResource>.Resource r = pool.acquire()) {
            assertSame(resource, r.get());
        }
    }

    @Test
    public void testCloseClosesResources() throws Exception {
        ProtectedResource a = spy(new ProtectedResource());

        CloseablePool<ProtectedResource> pool = new CloseablePool<>(() -> a, 1);

        pool.close();

        verify(a).close();
    }

    @Test(timeout = 500)
    public void testCloseWaitsForAcquiredResourcesToBeReleased() throws Exception {
        final ProtectedResource a = spy(new ProtectedResource());
        final CloseablePool<ProtectedResource> pool = new CloseablePool<>(() -> a, 1);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean awoken = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                try (CloseablePool<ProtectedResource>.Resource _r = pool.acquire()) {
                    latch.countDown();
                    Thread.sleep(20);
                    awoken.set(true);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();

        pool.close();

        assertTrue(awoken.get());
        verify(a).close();
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test(expected = CloseablePool.PoolClosedException.class, timeout = 500)
    public void testExceptionThrownIfAcquireAfterClose() throws Exception {
        final ProtectedResource r = new ProtectedResource();
        final CloseablePool<ProtectedResource> pool = new CloseablePool<>(() -> r, 1);

        pool.close();

        pool.acquire();
    }

    @SuppressWarnings("MustBeClosedChecker")
    @Test(expected = CloseablePool.PoolClosedException.class, timeout = 500)
    public void testAcquireThrowsExceptionIfClosedWhileWaiting() throws Exception {
        final ProtectedResource a = new ProtectedResource();
        final CloseablePool<ProtectedResource> pool = new CloseablePool<>(() -> a, 1);
        final CountDownLatch acquiredLatch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                try (CloseablePool<ProtectedResource>.Resource _r = pool.acquire()) {
                    acquiredLatch.countDown();
                    // Let close complete after 50ms so the thread doesn't run forever
                    Thread.sleep(50);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                acquiredLatch.await();
                // Give the main thread a chance to try and acquire it
                Thread.sleep(20);
                pool.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        acquiredLatch.await();

        pool.acquire();
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionThrownIfGettingResultOfReleasedResource() throws InterruptedException {
        CloseablePool<ProtectedResource> pool = new CloseablePool<>(ProtectedResource::new, 1);

        CloseablePool<ProtectedResource>.Resource resource = pool.acquire();
        // Release it
        resource.close();

        resource.get();
    }

    @Test
    public void testAllResourcesClosedEvenIfOneThrowsException() throws Exception {
        ProtectedResource a = mock(ProtectedResource.class);
        ProtectedResource b = mock(ProtectedResource.class);

        Exception aExpected = new Exception();
        Exception bExpected = new Exception();

        doThrow(aExpected).when(a).close();
        doThrow(bExpected).when(b).close();

        Iterator<ProtectedResource> resourceIterator = List.of(a, b).iterator();

        CloseablePool<ProtectedResource> pool = new CloseablePool<>(resourceIterator::next, 2);

        try {
            pool.close();
            fail("Close should have thrown contained exceptions");
        } catch (Exception e) {
            verify(a).close();
            verify(b).close();

            if (e == aExpected) {
                assertArrayEquals(e.getSuppressed(), new Exception[]{bExpected});
            } else {
                assertSame(bExpected, e);
                assertArrayEquals(e.getSuppressed(), new Exception[]{aExpected});
            }
        }
    }

    private class ProtectedResource implements AutoCloseable {
        @Override
        public void close() throws Exception { }
    }
}
