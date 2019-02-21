package net.jackw.olep.message.modification;

import com.google.common.base.MoreObjects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class ModificationKey {
    public final long transactionId;
    public final short modificationId;

    public ModificationKey(long transactionId, short modificationId) {
        this.transactionId = transactionId;
        this.modificationId = modificationId;
    }

    public static class KeySerializer implements Serializer<ModificationKey> {

        @Override
        public void configure(Map configs, boolean isKey) { }

        @Override
        public byte[] serialize(String topic, ModificationKey data) {
            return ByteBuffer.allocate(12).putLong(data.transactionId).putShort(data.modificationId).array();
        }

        @Override
        public void close() { }
    }

    public static class KeyDeserializer implements Deserializer<ModificationKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) { }

        @Override
        public ModificationKey deserialize(String topic, byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            return new ModificationKey(
                buffer.getLong(),
                buffer.getShort()
            );
        }

        @Override
        public void close() { }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("transactionId", transactionId)
            .add("modificationId", modificationId)
            .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, modificationId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ModificationKey) {
            ModificationKey other = (ModificationKey) obj;
            return transactionId == other.transactionId && modificationId == other.modificationId;
        }
        return false;
    }
}
