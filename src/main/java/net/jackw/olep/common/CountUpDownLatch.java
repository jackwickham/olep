package net.jackw.olep.common;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A latch that can count up and down
 *
 * Calling await will block until the count becomes zero.
 *
 * This latch preserves the invariant that the count must always be non-negative.
 */
public class CountUpDownLatch {
    private final AtomicInteger count;

    /**
     * Create a new latch with the given count
     *
     * @param initialCount The initial value of the counter
     */
    public CountUpDownLatch(int initialCount) {
        Preconditions.checkArgument(initialCount >= 0, "Count must be non-negative");
        this.count = new AtomicInteger(initialCount);
    }

    /**
     * Decrement the counter
     *
     * If the counter reaches zero, waiting threads will be woken up. If the counter subsequently increases before the
     * waiting threads have left `await`, it is unspecified as to whether that thread will resume this time.
     *
     * If the count is currently zero, this method will not change it.
     */
    public void countDown() {
        int newValue = count.updateAndGet(v -> v == 0 ? 0 : v - 1);
        if (newValue == 0) {
            synchronized (count) {
                count.notifyAll();
            }
        }
    }

    /**
     * Increment the counter
     */
    public void countUp() {
        count.incrementAndGet();
    }

    /**
     * Wait for the counter to become zero
     *
     * It is *not* guaranteed that the counter will still be zero upon return from this method, but it is guaranteed
     * that it was zero at some point.
     *
     * @throws InterruptedException If the current thread is interrupted while waiting
     */
    public void await() throws InterruptedException {
        synchronized (count) {
            while (count.get() > 0) {
                count.wait();
            }
        }
    }

    /**
     * Wait up to approximately the provided duration for the counter to become zero
     *
     * @param timeout The maximum time to wait
     * @param unit The units of the timeout
     * @return true if the latch reached zero, and false otherwise
     * @throws InterruptedException If the current thread is interrupted while waiting
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        long end = System.nanoTime() + unit.toNanos(timeout);
        synchronized (count) {
            while (count.get() > 0) {
                long remaining = end - System.nanoTime();
                if (remaining < 0) {
                    return false;
                } else {
                    count.wait(remaining / 1000000, (int)(remaining % 1000000));
                }
            }
            return true;
        }
    }

    /**
     * Get the current value of the counter
     */
    public int getCount() {
        return count.get();
    }
}
