package net.jackw.olep.common.store;

import com.google.common.io.Files;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import net.jackw.olep.common.KafkaConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.function.Supplier;

public class DiskBackedMapStore<K, V> implements WritableKeyValueStore<K, V>, AutoCloseable {
    @GuardedBy("DiskBackedMapStore.class")
    private static HashMap<String, DiskBackedMapStore<?, ?>> instances = new HashMap<>(6);

    private ChronicleMap<K, V> map;
    private String storeName;

    @GuardedBy("DiskBackedMapStore.class")
    private int references = 1;

    private DiskBackedMapStore(ChronicleMapBuilder<K, V> mapBuilder, String storeName) {
        try {
            File persistentFile = new File(KafkaConfig.storeBackingDir(), storeName + ".dat");
            Files.createParentDirs(persistentFile);

            this.map = mapBuilder.createPersistedTo(persistentFile);
            this.storeName = storeName;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Get the store with the provided name, creating it if it doesn't exist
     *
     * @param storeName The name of the store
     * @param create A supplier that creates the store on demand
     * @return The store
     */
    @SuppressWarnings("unchecked")
    private static synchronized <K, V> DiskBackedMapStore<K, V> getOrCreate(String storeName, Supplier<DiskBackedMapStore<K, V>> create) {
        DiskBackedMapStore<K, V> instance = (DiskBackedMapStore<K, V>) instances.get(storeName);
        if (instance == null) {
            instance = create.get();
            instances.put(storeName, instance);

            // TODO: Check whether we really want to delete all the old data like this
            instance.map.clear();
        }

        instance.references++;
        return instance;
    }

    /**
     * Get an instance of the store with the provided name and properties
     *
     * @param capacity The <b>maximum</b> number of entries that the store can contain
     * @param keyClass The type of the store key
     * @param valueClass The type of the store value
     * @param storeName The string name for the store
     * @param averageKey A representative instance of the keys
     * @param averageValue A representative instance of the values
     * @return An instance of the store
     */
    public static <K, V> DiskBackedMapStore<K, V> create(long capacity, Class<K> keyClass, Class<V> valueClass,
                                                        String storeName, K averageKey, V averageValue) {
        return getOrCreate(storeName, () -> {
            ChronicleMapBuilder<K, V> mapBuilder = ChronicleMapBuilder.of(keyClass, valueClass)
                .entries(capacity)
                .name(storeName)
                .averageKey(averageKey)
                .averageValue(averageValue);

            return new DiskBackedMapStore<>(mapBuilder, storeName);
        });
    }


    /**
     * Get an instance of the store with the provided name and properties, which is keyed by an integer
     *
     * @param capacity The <b>maximum</b> number of entries that the store can contain
     * @param valueClass The type of the store value
     * @param storeName The string name for the store
     * @param averageValue A representative instance of the values
     * @return An instance of the store
     */
    public static <V> DiskBackedMapStore<Integer, V> createIntegerKeyed(long capacity, Class<V> valueClass,
                                                                        String storeName, V averageValue) {
        return getOrCreate(storeName, () -> {
            ChronicleMapBuilder<Integer, V> mapBuilder = ChronicleMapBuilder.of(Integer.class, valueClass)
                .entries(capacity)
                .name(storeName)
                .averageValue(averageValue);

            return new DiskBackedMapStore<>(mapBuilder, storeName);
        });
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

    @Nullable
    @Override
    public V put(K key, @Nonnull V value) {
        return map.put(key, value);
    }

    @Nullable
    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    /**
     * Close this store
     *
     * This method must be called exactly once per call to create/createIntegerKeyed
     */
    @Override
    public void close() {
        synchronized (DiskBackedMapStore.class) {
            if (--references == 0) {
                instances.remove(storeName);
                map.close();
            }
        }
    }

    /**
     * Remove everything from the map
     */
    @Override
    public void clear() {
        map.clear();
    }
}
