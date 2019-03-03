package net.jackw.olep.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A latch that can be open or closed, and is thread safe
 *
 * This is useful for checking cross-thread conditions that must be satisfied for an operation to succeed, and which
 * may transition between satisfied and not satisfied. Calling await will block until the latch becomes closed, at which
 * point the condition has been satisfied.
 */
public class Latch {
    private final AtomicBoolean closed;

    /**
     * Construct a new Latch, with the initial state
     *
     * @param initiallyClosed Whether the latch should initially be closed (allowing flow to pass)
     */
    public Latch(boolean initiallyClosed) {
        closed = new AtomicBoolean(initiallyClosed);
    }

    /**
     * Open the latch, to prevent threads that await the latch from passing
     */
    public void open() {
        closed.set(false);
    }

    /**
     * Close the latch, to allow control flow to pass
     */
    public void close() {
        boolean oldValue = closed.getAndSet(true);
        if (!oldValue) {
            synchronized (closed) {
                closed.notifyAll();
            }
        }
    }

    /**
     * Wait for the latch to become closed
     *
     * @throws InterruptedException If the current thread is interrupted while waiting
     */
    public void await() throws InterruptedException {
        synchronized (closed) {
            while (!closed.get()) {
                closed.wait();
            }
        }
    }

    /**
     * Wait for the latch to become closed, with a timeout
     *
     * @param timeout The duration to wait
     * @param unit The units of the wait
     * @throws InterruptedException If the current thread is interrupted while waiting
     * @throws TimeoutException If the timeout is reached before the latch is opened
     */
    public void await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        long end = System.nanoTime() + unit.toNanos(timeout);
        synchronized (closed) {
            while (!closed.get()) {
                long remaining = end - System.nanoTime();
                if (remaining <= 0) {
                    throw new TimeoutException();
                } else {
                    closed.wait(remaining / 1000000, (int)(remaining % 1000000));
                }
            }
        }
    }

    /**
     * Get the current state of the latch
     */
    public boolean isClosed() {
        return closed.get();
    }
}
