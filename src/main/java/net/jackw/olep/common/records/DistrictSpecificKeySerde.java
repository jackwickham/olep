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

public class DistrictSpecificKeySerde implements Serializer<DistrictSpecificKey>, Deserializer<DistrictSpecificKey>,
    SizedReader<DistrictSpecificKey>, SizedWriter<DistrictSpecificKey>, ReadResolvable<DistrictSpecificKeySerde>,
    Serde<DistrictSpecificKey>
{
    private static final DistrictSpecificKeySerde INSTANCE = new DistrictSpecificKeySerde();

    private DistrictSpecificKeySerde() { }

    @NotNull
    @Override
    public DistrictSpecificKey read(Bytes in, long size, @Nullable DistrictSpecificKey using) {
        // DistrictSpecificKey is final, so using isn't useful
        return new DistrictSpecificKey(in.readInt(), in.readInt(), in.readInt());
    }

    @Override
    public void write(Bytes out, long size, @NotNull DistrictSpecificKey toWrite) {
        out.writeInt(toWrite.id)
            .writeInt(toWrite.districtId)
            .writeInt(toWrite.warehouseId);
    }

    @Override
    public long size(@NotNull DistrictSpecificKey toWrite) {
        return 12;
    }

    @Override
    public DistrictSpecificKey deserialize(String topic, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return new DistrictSpecificKey(buffer.getInt(), buffer.getInt(), buffer.getInt());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, DistrictSpecificKey data) {
        return ByteBuffer.allocate(12)
            .putInt(data.id)
            .putInt(data.districtId)
            .putInt(data.warehouseId)
            .array();
    }

    @Override
    public void close() { }

    @Override
    public Serializer<DistrictSpecificKey> serializer() {
        return this;
    }

    @Override
    public Deserializer<DistrictSpecificKey> deserializer() {
        return this;
    }

    @NotNull
    @Override
    public DistrictSpecificKeySerde readResolve() {
        return INSTANCE;
    }

    public static DistrictSpecificKeySerde getInstance() {
        return INSTANCE;
    }
}
