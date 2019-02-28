package net.jackw.olep.common;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class CountUpDownLatchTest {
    @Test(expected = IllegalArgumentException.class)
    public void testConstructionThrowsExceptionOnNegativeInitialCount() {
        new CountUpDownLatch(-1);
    }

    @Test
    public void testGetCountReturnsCurrentValue() {
        CountUpDownLatch latch = new CountUpDownLatch(2);
        assertEquals(2, latch.getCount());
    }

    @Test
    public void testCountDownDecrementsCount() {
        CountUpDownLatch latch = new CountUpDownLatch(2);
        latch.countDown();
        assertEquals(1, latch.getCount());
    }

    @Test
    public void testCountUpIncrementsCount() {
        CountUpDownLatch latch = new CountUpDownLatch(2);
        latch.countUp();
        assertEquals(3, latch.getCount());
    }

    @Test
    public void testAwaitWhenCountInitiallyZeroDoesntBlock() throws InterruptedException {
        CountUpDownLatch latch = new CountUpDownLatch(0);
        Thread.currentThread().interrupt();
        latch.await();
        assertTrue(Thread.interrupted());
    }

    @Test
    public void testAwaitWhenCountBecameZeroDoesntBlock() throws InterruptedException {
        CountUpDownLatch latch = new CountUpDownLatch(1);
        latch.countDown();
        Thread.currentThread().interrupt();
        latch.await();
        assertTrue(Thread.interrupted());
    }

    @Test(expected = InterruptedException.class)
    public void testAwaitWhenCountPositiveDoesBlock() throws InterruptedException {
        CountUpDownLatch latch = new CountUpDownLatch(1);
        Thread.currentThread().interrupt();
        latch.await();
    }

    @Test(timeout = 100L)
    public void testAwaitingThreadsReleasedWhenThreadBecomesZero() throws InterruptedException {
        final CountUpDownLatch latch = new CountUpDownLatch(2);
        final AtomicBoolean beenReleased = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                Thread.sleep(20);
                latch.countDown();

                Thread.sleep(20);
                beenReleased.set(true);
                latch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }, "latch-release-thread").start();

        latch.await();
        assertTrue(beenReleased.get());
    }

    @Test(timeout = 100L)
    public void testAwaitingThreadsNotReleasedWhenCountedUp() throws InterruptedException {
        final CountUpDownLatch latch = new CountUpDownLatch(1);
        final AtomicBoolean beenReleased = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                Thread.sleep(10);
                latch.countUp(); // 2

                Thread.sleep(10);
                latch.countDown(); // 1

                Thread.sleep(10);
                latch.countUp(); // 2

                Thread.sleep(10);
                latch.countDown(); // 1

                Thread.sleep(10);
                beenReleased.set(true);
                latch.countDown(); // 0
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }, "latch-release-thread").start();

        latch.await();
        assertTrue(beenReleased.get());
    }
}
