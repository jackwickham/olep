package net.jackw.olep.common.store;

import com.google.common.io.Files;
import net.jackw.olep.common.DatabaseConfig;
import net.openhft.chronicle.bytes.BytesMarshaller;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
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
    private static Random rand = new Random();

    private DiskBackedMapStore(ChronicleMapBuilder<K, V> mapBuilder, String storeName, DatabaseConfig config) {
        try {
            File persistentFile = new File(config.getStoreBackingDir(), storeName + rand.nextInt() + ".dat");
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
     * @param config The current database configuration
     * @return An instance of the store
     */
    public static <K, V, KM extends SizedReader<K> & SizedWriter<? super K>, VM extends BytesReader<V> & BytesWriter<? super V>> DiskBackedMapStore<K, V> create(
        long capacity, Class<K> keyClass, Class<V> valueClass, String storeName, K averageKey, V averageValue,
        DatabaseConfig config, KM keyMarshaller//, VM valueMarshaller
    ) {
        ChronicleMapBuilder<K, V> mapBuilder = ChronicleMapBuilder.of(keyClass, valueClass)
            .entries(capacity)
            .name(storeName + rand.nextInt())
            .averageKey(averageKey)
            .averageValue(averageValue)
            .keyMarshaller(keyMarshaller)
            //.valueMarshaller(valueMarshaller)
            .putReturnsNull(true)
            .removeReturnsNull(true);

        return new DiskBackedMapStore<>(mapBuilder, storeName, config);
    }


    /**
     * Get an instance of the store with the provided name and properties, which is keyed by an integer
     *
     * @param capacity The <b>maximum</b> number of entries that the store can contain
     * @param valueClass The type of the store value
     * @param storeName The string name for the store
     * @param averageValue A representative instance of the values
     * @param config The current database configuration
     * @return An instance of the store
     */
    public static <V> DiskBackedMapStore<Integer, V> createIntegerKeyed(long capacity, Class<V> valueClass,
                                                                        String storeName, V averageValue,
                                                                        DatabaseConfig config) {
        ChronicleMapBuilder<Integer, V> mapBuilder = ChronicleMapBuilder.of(Integer.class, valueClass)
            .entries(capacity)
            .name(storeName + rand.nextInt())
            .averageValue(averageValue)
            .putReturnsNull(true)
            .removeReturnsNull(true);

        return new DiskBackedMapStore<>(mapBuilder, storeName, config);
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
