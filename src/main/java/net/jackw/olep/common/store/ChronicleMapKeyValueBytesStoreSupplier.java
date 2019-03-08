package net.jackw.olep.common.store;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class ChronicleMapKeyValueBytesStoreSupplier<K, V> implements KeyValueBytesStoreSupplier {
    private final String name;
    private final int maxEntries;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final K averageKey;
    private final V averageValue;

    public ChronicleMapKeyValueBytesStoreSupplier(
        String name, int maxEntries, Serializer<K> keySerializer, Serializer<V> valueSerializer, K averageKey,
        V averageValue
    ) {
        this.name = name;
        this.maxEntries = maxEntries;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.averageKey = averageKey;
        this.averageValue = averageValue;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return ChronicleMapKafkaStateStore.create(name, maxEntries, keySerializer, valueSerializer, averageKey, averageValue);
    }

    @Override
    public String metricsScope() {
        return "chroniclemap-state";
    }
}
