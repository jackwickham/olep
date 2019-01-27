package net.jackw.olep.common;

import com.google.common.io.Files;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

public class DiskBackedMapStore<K, V> implements WritableKeyValueStore<K, V>, AutoCloseable {
    private ChronicleMap<K, V> map;

    public DiskBackedMapStore(long capacity, Class<K> keyClass, Class<V> valueClass, String storeName, K averageKey, V averageValue) {
        try {
            File persistentFile = new File(KafkaConfig.storeBackingDir(), storeName + ".dat");
            Files.createParentDirs(persistentFile);
            map = ChronicleMapBuilder.of(keyClass, valueClass)
                .entries(capacity)
                .name(storeName)
                .averageKey(averageKey)
                .averageValue(averageValue)
                .createPersistedTo(persistentFile);
            // TODO: This may not be ideal
            // This restricts us to one instance of the store per disk
            map.clear();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    @Override
    public void close() {
        map.close();
    }
}
