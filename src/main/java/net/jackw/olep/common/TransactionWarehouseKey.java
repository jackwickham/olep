package net.jackw.olep.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

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
        private ByteBuffer buffer = ByteBuffer.allocate(12);

        @Override
        public void configure(Map configs, boolean isKey) { }

        @Override
        public byte[] serialize(String topic, TransactionWarehouseKey data) {
            return buffer.rewind().putLong(data.transactionId).putInt(data.warehouseId).array();
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
}
