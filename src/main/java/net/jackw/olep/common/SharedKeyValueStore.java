package net.jackw.olep.common;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface SharedKeyValueStore<K, V> {
    int DEFAULT_BACKOFF_ATTEMPTS = 20;

    /**
     * Tests whether the specified key has a corresponding value in this store
     *
     * @param key The key to check
     * @return true if there is a value in the table for the key, and false otherwise
     */
    boolean containsKey(K key);

    /**
     * Get the value associated with the requested key
     *
     * @param key The key to retrieve the value of
     * @return The value associated with that key, or null if there is no entry for that key
     */
    @Nullable
    V get(K key);

    /**
     * Get the value associated with the key, blocking until the value becomes available. Blocking time is limited, and
     * if the key is still not present a {@link StoreKeyMissingException} will be thrown.
     *
     * @param key The key to retrieve the value of
     * @param maxAttempts The number of times to retry, or 0 for unlimited
     * @return The value associated with that key
     */
    @Nonnull
    default V getBlocking(K key, int maxAttempts) throws InterruptedException {
        int attempts = 0;
        V result;
        do {
            while (!containsKey(key)) {
                if (maxAttempts > 0 && attempts >= maxAttempts) {
                    throw new StoreKeyMissingException(key);
                }
                Thread.sleep(50);
                attempts++;
            }
            result = get(key);
        } while (result == null);
        return result;
    }

    /**
     * Get the value associated with the key, blocking until the value becomes available. Blocking time is limited, and
     * if the key is still not present a {@link StoreKeyMissingException} will be thrown.
     *
     * 20 attempts will be made (for a maximum blocking time of approximately 1 second) before the exception is thrown.
     *
     * @param key The key to retrieve the value of
     * @return The value associated with that key
     */
    @Nonnull
    default V getBlocking(K key) throws InterruptedException {
        return getBlocking(key, DEFAULT_BACKOFF_ATTEMPTS);
    }
}
