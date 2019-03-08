package net.jackw.olep.common.store;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ChronicleMapKafkaStateStore implements KeyValueStore<Bytes, byte[]>, RecordBatchingStateRestoreCallback {
    private final String name;
    private ChronicleMap<byte[], byte[]> map;
    private final ChronicleMapBuilder<byte[], byte[]> mapBuilder;

    private ChronicleMapKafkaStateStore(String name, ChronicleMapBuilder<byte[], byte[]> mapBuilder) {
        this.name = name;
        this.mapBuilder = mapBuilder;
    }

    public static <K, V> ChronicleMapKafkaStateStore create(
        String name, int maxEntries, Serializer<K> keySerializer, Serializer<V> valueSerializer, K averageKey,
        V averageValue
    ) {
        byte[] serializedKey = keySerializer.serialize(null, averageKey);
        byte[] serializedValue = valueSerializer.serialize(null, averageValue);
        ChronicleMapBuilder<byte[], byte[]> mapBuilder = ChronicleMapBuilder.of(byte[].class, byte[].class)
            .averageKey(serializedKey)
            .averageValue(serializedValue)
            .entries(maxEntries)
            // Returning null violates the KeyValueStore contract, but we don't need it
            .putReturnsNull(true)
            .removeReturnsNull(true);

        return new ChronicleMapKafkaStateStore(name, mapBuilder);
    }

    private void openDb(File stateStoreDir) {
        try {
            File persistentFile = new File(stateStoreDir, name + ".dat");

            this.map = mapBuilder.createPersistedTo(persistentFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void put(Bytes key, byte[] value) {
        map.put(key.get(), value);
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        return map.putIfAbsent(key.get(), value);
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        map.putAll(entries.stream().collect(Collectors.toMap(kv -> kv.key.get(), kv -> kv.value)));
    }

    @Override
    public byte[] delete(Bytes key) {
        return map.remove(key.get());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        openDb(context.stateDir());

        context.register(root, this);
    }

    @Override
    public void flush() { }

    @Override
    public void close() {
        map.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return map != null;
    }

    @Override
    public byte[] get(Bytes key) {
        return map.get(key.get());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long approximateNumEntries() {
        return map.size();
    }

    @Override
    public void restoreBatch(Collection<ConsumerRecord<byte[], byte[]>> records) {
        map.putAll(records.stream().collect(Collectors.toMap(ConsumerRecord::key, ConsumerRecord::value)));
    }

    @Override
    public void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
        map.putAll(records.stream().collect(Collectors.toMap(kv -> kv.key, kv -> kv.value)));
    }

    @Override
    public void restore(byte[] key, byte[] value) {
        map.put(key, value);
    }
}
