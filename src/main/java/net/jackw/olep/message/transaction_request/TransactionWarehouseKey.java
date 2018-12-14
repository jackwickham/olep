package net.jackw.olep.message.transaction_request;

import com.google.common.base.MoreObjects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/**
 * The key for an accepted transaction that should be routed to a particular worker
 */
public class TransactionWarehouseKey {
    public final long transactionId;
    public final int warehouseId;

    public TransactionWarehouseKey(long transactionId, int warehouseId) {
        this.transactionId = transactionId;
        this.warehouseId = warehouseId;
    }

    public static class KeySerializer implements Serializer<TransactionWarehouseKey> {

        @Override
        public void configure(Map configs, boolean isKey) { }

        @Override
        public byte[] serialize(String topic, TransactionWarehouseKey data) {
            return ByteBuffer.allocate(12).putLong(data.transactionId).putInt(data.warehouseId).array();
        }

        @Override
        public void close() { }
    }

    public static class KeyDeserializer implements Deserializer<TransactionWarehouseKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) { }

        @Override
        public TransactionWarehouseKey deserialize(String topic, byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            return new TransactionWarehouseKey(
                buffer.getLong(),
                buffer.getInt()
            );
        }

        @Override
        public void close() { }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("transactionId", transactionId)
            .add("warehouseId", warehouseId)
            .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, warehouseId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TransactionWarehouseKey) {
            TransactionWarehouseKey other = (TransactionWarehouseKey) obj;
            return transactionId == other.transactionId && warehouseId == other.warehouseId;
        } else {
            return false;
        }
    }
}
