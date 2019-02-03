package net.jackw.olep.common.store;

import com.google.common.io.Files;
import net.jackw.olep.common.KafkaConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Random;

public class DiskBackedMapStore<K, V> implements WritableKeyValueStore<K, V>, AutoCloseable {
    private ChronicleMap<K, V> map;
    private String storeName;

    private DiskBackedMapStore(ChronicleMapBuilder<K, V> mapBuilder, String storeName) {
        Random rand = new Random();
        try {
            File persistentFile = new File(KafkaConfig.storeBackingDir(), storeName + rand.nextInt() + ".dat");
            Files.createParentDirs(persistentFile);
            persistentFile.deleteOnExit();

            this.map = mapBuilder.createPersistedTo(persistentFile);
            this.storeName = storeName;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        ChronicleMapBuilder<K, V> mapBuilder = ChronicleMapBuilder.of(keyClass, valueClass)
            .entries(capacity)
            .name(storeName + new Random().nextInt())
            .averageKey(averageKey)
            .averageValue(averageValue);

        return new DiskBackedMapStore<>(mapBuilder, storeName);
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
        ChronicleMapBuilder<Integer, V> mapBuilder = ChronicleMapBuilder.of(Integer.class, valueClass)
            .entries(capacity)
            .name(storeName + new Random().nextInt())
            .averageValue(averageValue);

        return new DiskBackedMapStore<>(mapBuilder, storeName);
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
     */
    @Override
    public void close() {
        map.close();
    }

    /**
     * Remove everything from the map
     */
    @Override
    public void clear() {
        map.clear();
    }
}
