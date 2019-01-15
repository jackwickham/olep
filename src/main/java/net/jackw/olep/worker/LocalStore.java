package net.jackw.olep.worker;

import org.apache.kafka.streams.state.KeyValueStore;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A wrapper around a worker-local key-value store to provide a default argument if the underlying store returns null
 */
public class LocalStore<K, V> {
    private KeyValueStore<K, V> store;
    private Supplier<V> defaultValueSupplier;

    /**
     * Create a new LocalStore with a function that generates new values
     */
    public LocalStore(KeyValueStore<K, V> store, Supplier<V> defaultValueSupplier) {
        this.store = store;
        this.defaultValueSupplier = defaultValueSupplier;
    }

    /**
     * Create a new LocalStore with a default value
     */
    public LocalStore(KeyValueStore<K, V> store, V defaultValue) {
        this(store, () -> defaultValue);
    }

    /**
     * Get the value, returning the default value if the underlying store returns null
     */
    @Nonnull
    public V get(K key) {
        V result = store.get(key);
        if (result == null) {
            result = defaultValueSupplier.get();
        }
        return result;
    }

    /**
     * Store a value into the store
     */
    public void put(K key, V value) {
        store.put(key, value);
    }

    /**
     * Get the underlying store
     */
    public KeyValueStore<K, V> getStore() {
        return store;
    }
}
