package net.jackw.olep.common;

public interface SharedKeyValueStore<K, V> {
    /**
     * Tests whether the specified key has a corresponding value in this store
     *
     * @param key The key to check
     * @return true if there is a value in the table for the key, and false otherwise
     */
    boolean contains(K key);

    /**
     * Get the value associated with the requested key
     *
     * @param key The key to retrieve the value of
     * @return The value associated with that key (potentially null), or null if there is no entry for that key
     */
    V get(K key);
}
