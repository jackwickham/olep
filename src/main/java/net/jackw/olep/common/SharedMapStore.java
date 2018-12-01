package net.jackw.olep.common;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.util.concurrent.ConcurrentHashMap;

public class SharedMapStore<K, V> implements SharedKeyValueStore<K, V> {
    private ConcurrentHashMap<K, V> map;

    public SharedMapStore(int initialCapacity) {
        map = new ConcurrentHashMap<>(initialCapacity);
    }

    @Override
    public boolean contains(K key) {
        return map.containsKey(key);
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @CanIgnoreReturnValue
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @CanIgnoreReturnValue
    public V remove(K key) {
        return map.remove(key);
    }
}
