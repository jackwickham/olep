package net.jackw.olep.common;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A wrapper around a ReadOnlyKeyValueStore which makes sure the streams app is in the right state, and retries up to 5
 * times on error
 *
 * @param <K> The key value store's key type
 * @param <V> The key value store's value type
 */
public class SafeReadOnlyKeyValueStore<K, V> {
    private final ReadOnlyKeyValueStore<K, V> store;
    private final Latch streamsRunningLatch;

    private static final int MAX_ATTEMPTS = 5;
    private static final int LATCH_TIMEOUT_MS = 2000;

    public SafeReadOnlyKeyValueStore(KafkaStreams streams, String storeName, Latch streamsRunningLatch) throws InterruptedException, StoreUnavailableException {
        this.streamsRunningLatch = streamsRunningLatch;
        this.store = attempt(() -> streams.store(storeName, QueryableStoreTypes.keyValueStore()));
    }

    /**
     * Get a value from the store
     *
     * @param key The store key
     * @return The value associated with that key
     * @throws InterruptedException If the current thread is interrupted while waiting for the store to become available
     * @throws StoreUnavailableException If a timeout occurs or the number of attempts is exceeded
     */
    public V get(K key) throws InterruptedException, StoreUnavailableException {
        return attempt(() -> store.get(key));
    }

    /**
     * Try to perform task when the latch is closed, catching InvalidStateStoreExceptions and retrying
     * @param task The task to perform
     * @param <R> The result type of the task
     * @return The result of performing the task
     * @throws InterruptedException If the current thread is interrupted while waiting for the store to become available
     * @throws StoreUnavailableException If a timeout occurs or the number of attempts is exceeded
     */
    private <R> R attempt(Supplier<R> task) throws InterruptedException, StoreUnavailableException {
        List<Exception> suppressedExceptions = new ArrayList<>(5);
        for (int i = 0 ;; i++) {
            try {
                streamsRunningLatch.await(LATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                return task.get();
            } catch (TimeoutException e) {
                throw new StoreUnavailableException(e, suppressedExceptions);
            } catch (InvalidStateStoreException e) {
                if (i >= MAX_ATTEMPTS) {
                    throw new StoreUnavailableException(e, suppressedExceptions);
                } else {
                    suppressedExceptions.add(e);
                }
            }
        }
    }

    public static class StoreUnavailableException extends Exception {
        public StoreUnavailableException(Throwable cause, List<? extends Throwable> suppressed) {
            super(cause);
            for (Throwable e : suppressed) {
                addSuppressed(e);
            }
        }
    }
}
