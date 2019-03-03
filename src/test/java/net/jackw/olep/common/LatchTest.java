package net.jackw.olep.common;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class LatchTest {
    @Test
    public void testConstructingWithTrueIsInitiallyClosed() {
        Latch latch = new Latch(true);
        assertTrue(latch.isClosed());
    }

    @Test
    public void testConstructingWithFalseIsInitiallyOpen() {
        Latch latch = new Latch(false);
        assertFalse(latch.isClosed());
    }

    @Test
    public void testOpenSetsLatchOpen() {
        Latch latch = new Latch(true);
        latch.open();
        assertFalse(latch.isClosed());
    }

    @Test
    public void testCloseSetsLatchClosed() {
        Latch latch = new Latch(false);
        latch.close();
        assertTrue(latch.isClosed());
    }

    @Test(timeout = 100)
    public void testAwaitDoesntBlockWhenClosed() throws InterruptedException {
        Latch latch = new Latch(true);
        Thread.currentThread().interrupt();
        latch.await();
        assertTrue(Thread.interrupted());
    }

    @Test(timeout = 100)
    public void testAwaitReleasedWhenLatchBecomesClosed() throws InterruptedException {
        Latch latch = new Latch(false);
        AtomicBoolean closed = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                Thread.sleep(10);
                closed.set(true);
                latch.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();
        assertTrue(closed.get());
    }

    @Test(expected = TimeoutException.class, timeout = 100)
    public void testTimeout() throws InterruptedException, TimeoutException {
        Latch latch = new Latch(false);
        latch.await(50, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 100)
    public void testAwaitWithTimeoutReleasedWhenLatchBecomesClosed() throws InterruptedException, TimeoutException {
        Latch latch = new Latch(false);
        AtomicBoolean closed = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                Thread.sleep(10);
                closed.set(true);
                latch.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await(50, TimeUnit.MILLISECONDS);
        assertTrue(closed.get());
    }
}
