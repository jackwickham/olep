package net.jackw.olep.common;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;

public class SharedMapStore<K, V> implements SharedKeyValueStore<K, V> {
    private ConcurrentHashMap<K, V> map;
    private final int BACKOFF_ATTEMPTS = 10;

    public SharedMapStore(int initialCapacity) {
        map = new ConcurrentHashMap<>(initialCapacity);
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

    @Override
    @Nonnull
    public V getBlocking(K key) throws InterruptedException {
        int attempts = 0;
        V result;
        do {
            while (!map.containsKey(key)) {
                if (attempts >= BACKOFF_ATTEMPTS) {
                    throw new StoreKeyMissingException(key);
                }
                Thread.sleep(100 + attempts++);
            }
            result = map.get(key);
        } while (result == null);
        return result;
    }

    /**
     * Save a value into the store with the given key
     */
    @CanIgnoreReturnValue
    public V put(K key, @Nonnull V value) {
        return map.put(key, value);
    }

    @CanIgnoreReturnValue
    public V remove(K key) {
        return map.remove(key);
    }
}
