package net.jackw.olep.common.records;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Map;

public class WarehouseSpecificKeySerde implements Serializer<WarehouseSpecificKey>, Deserializer<WarehouseSpecificKey>,
    SizedReader<WarehouseSpecificKey>, SizedWriter<WarehouseSpecificKey>, ReadResolvable<WarehouseSpecificKeySerde>,
    Serde<WarehouseSpecificKey>
{
    private static final WarehouseSpecificKeySerde INSTANCE = new WarehouseSpecificKeySerde();

    private WarehouseSpecificKeySerde() { }

    @NotNull
    @Override
    public WarehouseSpecificKey read(Bytes in, long size, @Nullable WarehouseSpecificKey using) {
        // WarehouseSpecificKey is final, so using isn't useful
        return new WarehouseSpecificKey(in.readInt(), in.readInt());
    }

    @Override
    public void write(Bytes out, long size, @NotNull WarehouseSpecificKey toWrite) {
        out.writeInt(toWrite.id).writeInt(toWrite.warehouseId);
    }

    @Override
    public long size(@NotNull WarehouseSpecificKey toWrite) {
        return 8;
    }

    @Override
    public WarehouseSpecificKey deserialize(String topic, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return new WarehouseSpecificKey(buffer.getInt(), buffer.getInt());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, WarehouseSpecificKey data) {
        return ByteBuffer.allocate(8).putInt(data.id).putInt(data.warehouseId).array();
    }

    @Override
    public void close() { }

    @Override
    public Serializer<WarehouseSpecificKey> serializer() {
        return this;
    }

    @Override
    public Deserializer<WarehouseSpecificKey> deserializer() {
        return this;
    }

    @NotNull
    @Override
    public WarehouseSpecificKeySerde readResolve() {
        return INSTANCE;
    }

    public static WarehouseSpecificKeySerde getInstance() {
        return INSTANCE;
    }
}
