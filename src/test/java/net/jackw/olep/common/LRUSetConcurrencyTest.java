package net.jackw.olep.common;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

import static org.junit.Assert.*;

public class LRUSetConcurrencyTest {
    private LRUSet<Object> set;
    private static final int NUM_RUNS = 5000;

    @Test
    public void testConcurrentAddsOfSameElementOnlySucceedOnce() throws Throwable {
        final AtomicInteger successfulInsertionCount = new AtomicInteger();
        final Object o1 = new Object();

        concurrentTest(
            () -> {
                successfulInsertionCount.set(0);
                set = new LRUSet<>(3);
            },
            _i -> {
                if (set.add(o1)) {
                    successfulInsertionCount.incrementAndGet();
                }
            },
            () -> {
                assertEquals(1, successfulInsertionCount.get());
                assertEquals(1, set.size());
                assertThat(set, Matchers.contains(o1));
            },
            4
        );
    }

    @Test
    public void testMultipleConcurrentInsertsOfDifferentElementsAllSucceed() throws Throwable {
        final Object[] os = {new Object(), new Object(), new Object(), new Object()};

        concurrentTest(
            () -> {
                set = new LRUSet<>(4);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                assertEquals(4, set.size());
                assertThat(set, Matchers.containsInAnyOrder(os));
            },
            4
        );
    }

    @Test
    public void testConcurrentInsertionsAndAutomaticDeletionsPreserveInvariants() throws Throwable {
        final Object[] os = {new Object(), new Object()};
        final Object[] initialElements = {new Object(), new Object()};

        concurrentTest(
            () -> {
                set = new LRUSet<>(2);
                Collections.addAll(set, initialElements);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                assertThat(set, Matchers.containsInAnyOrder(os));
            },
            2
        );
    }

    @Test
    public void testElementInsertedAndDeleted() throws Throwable {
        final Object[] os = {new Object(), new Object(), new Object(), new Object()};

        concurrentTest(
            () -> {
                set = new LRUSet<>(1);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                assertEquals(1, set.size());
                assertThat(set, Matchers.contains(Matchers.in(os)));
            },
            4
        );
    }

    private void concurrentTest(Runnable setup, IntConsumer test, Runnable teardown, int numThreads) throws Throwable {
        AtomicReference<Throwable> error = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(numThreads);

        for (int run = 0; run < NUM_RUNS; run++) {
            final Thread[] threads = new Thread[numThreads];
            setup.run();

            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        barrier.await();
                        test.accept(threadId);
                    } catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[i].setUncaughtExceptionHandler((t, e) -> error.set(e));
                threads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            teardown.run();

            if (error.get() != null) {
                throw error.get();
            }
        }
    }
}
