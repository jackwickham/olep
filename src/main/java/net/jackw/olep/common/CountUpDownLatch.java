package net.jackw.olep.common;

import com.google.common.base.Preconditions;

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
     * @throws IllegalStateException If the latch would become negative. The count as externally observed may briefly
     *                               become negative, but will be incremented again, so this call has no long term
     *                               side effects
     */
    public void countDown() {
        int newValue = count.decrementAndGet();
        if (newValue == 0) {
            synchronized (count) {
                count.notifyAll();
            }
        } else if (newValue < 0) {
            count.incrementAndGet();
            throw new IllegalStateException("Latch dropped below 0");
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
     * Get the current value of the counter
     */
    public int getCount() {
        return count.get();
    }
}
