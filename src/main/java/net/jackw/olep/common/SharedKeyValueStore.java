package net.jackw.olep.common;

import javax.annotation.Nullable;

public interface SharedKeyValueStore<K, V> {
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
     * @return The value associated with that key (potentially null), or null if there is no entry for that key
     */
    @Nullable
    V get(K key);

    /**
     * Get the value associated with the key, blocking until the value becomes available. Blocking time is limited, and
     * if the key is still not present a {@link StoreKeyMissingException} will be thrown.
     *
     * @param key The key to retrieve the value of
     * @return The value associated with that key, which may be null
     */
    V getBlocking(K key) throws InterruptedException;
}
