package net.jackw.olep.common;

import com.google.errorprone.annotations.MustBeClosed;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class CloseablePool<T extends AutoCloseable> implements AutoCloseable {
    private Set<T> allResources;
    private Deque<T> availableResources;
    private boolean closed = false;

    public CloseablePool(Supplier<T> resourceCreator, int poolSize) {
        allResources = new HashSet<>(poolSize);
        availableResources = new ArrayDeque<>(poolSize);

        for (int i = 0; i < poolSize; i++) {
            T resource = resourceCreator.get();
            allResources.add(resource);
            availableResources.add(resource);
        }
    }

    @MustBeClosed
    public synchronized Resource acquire() throws InterruptedException {
        while (!closed && availableResources.isEmpty()) {
            wait();
        }
        if (closed) {
            throw new PoolClosedException();
        }
        T result = availableResources.removeLast();
        return new Resource(result);
    }

    private synchronized void release(T resource) {
        availableResources.add(resource);
        notify();
    }

    @Override
    public synchronized void close() throws Exception {
        Exception exception = null;

        closed = true;

        // Make all waiting threads throw closed exception
        notifyAll();

        while (!allResources.isEmpty()) {
            while (availableResources.isEmpty()) {
                wait();
            }

            T resource;
            while ((resource = availableResources.pollLast()) != null) {
                try {
                    allResources.remove(resource);
                    resource.close();
                } catch (Exception e) {
                    if (exception == null) {
                        exception = e;
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    public static class PoolClosedException extends RuntimeException { }

    public class Resource implements AutoCloseable {
        private final T resource;
        private boolean released = false;

        private Resource(T resource) {
            this.resource = resource;
        }

        public T get() {
            if (released) {
                throw new IllegalStateException("Can't call get on a resource that has been released already");
            }
            return resource;
        }

        @Override
        public void close() {
            if (!released) {
                released = true;
                release(resource);
            }
        }
    }
}
