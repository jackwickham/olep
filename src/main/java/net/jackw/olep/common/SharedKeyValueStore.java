package net.jackw.olep.common;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface SharedKeyValueStore<K, V> {
    int BACKOFF_ATTEMPTS = 10;

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
     * @return The value associated with that key
     */
    @Nonnull
    default V getBlocking(K key) throws InterruptedException {
        int attempts = 0;
        V result;
        do {
            while (!containsKey(key)) {
                if (attempts >= BACKOFF_ATTEMPTS) {
                    throw new StoreKeyMissingException(key);
                }
                Thread.sleep(100 + 100 * attempts++);
            }
            result = get(key);
        } while (result == null);
        return result;
    }
}
