package net.jackw.olep.common.store;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

class SharedMapStore<K, V> implements WritableKeyValueStore<K, V> {
    private Map<K, V> map;

    SharedMapStore(int initialCapacity) {
        map = new HashMap<>(initialCapacity);
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Nullable
    @Override
    public V get(K key) {
        return map.get(key);
    }

    /**
     * Save a value into the store with the given key, and return the previous value if it exists
     */
    @CanIgnoreReturnValue
    @Nullable
    @Override
    public V put(K key, @Nonnull V value) {
        return map.put(key, value);
    }

    /**
     * Remove the element with the given key from the store, returning the previous value if it existed
     */
    @CanIgnoreReturnValue
    @Nullable
    @Override
    public V remove(K key) {
        return map.remove(key);
    }
}
