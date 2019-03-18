package net.jackw.olep.common.store;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

interface WritableKeyValueStore<K, V> extends SharedKeyValueStore<K, V> {

    /**
     * Save a value into the store with the given key
     */
    void put(K key, @Nonnull V value);

    /**
     * Remove the element with the given key from the store, returning the previous value if it existed
     */
    @CanIgnoreReturnValue
    @Nullable
    V remove(K key);

    /**
     * Remove everything from the store
     */
    void clear();
}
