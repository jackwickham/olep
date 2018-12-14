package net.jackw.olep.message.transaction_result;

import com.google.errorprone.annotations.Immutable;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

@Immutable
public class TransactionResultKey {
    public final long transactionId;
    public final boolean approvalMessage;

    public TransactionResultKey(long transactionId, boolean approvalMessage) {
        this.transactionId = transactionId;
        this.approvalMessage = approvalMessage;
    }

    public int getConnectionId() {
        return (int) transactionId;
    }

    public int getTransactionSerialId() {
        return (int) (transactionId >>> 32);
    }

    public static class ResultKeyDeserializer implements Deserializer<TransactionResultKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) { }

        @Override
        public TransactionResultKey deserialize(String topic, byte[] data) {
            ByteBuffer buff = ByteBuffer.wrap(data);
            return new TransactionResultKey(
                buff.getLong(),
                buff.get() == 1
            );
        }

        @Override
        public void close() { }
    }

    public static class ResultKeySerializer implements Serializer<TransactionResultKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) { }

        @Override
        public byte[] serialize(String topic, TransactionResultKey data) {
            return ByteBuffer.allocate(9).putLong(data.transactionId).put(data.approvalMessage ? (byte) 1 : (byte) 0).array();
        }

        @Override
        public void close() { }
    }
}
