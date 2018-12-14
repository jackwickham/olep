package net.jackw.olep.message.transaction_result;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

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

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TransactionResultKey) {
            TransactionResultKey other = (TransactionResultKey) obj;
            return transactionId == other.transactionId && approvalMessage == other.approvalMessage;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, approvalMessage);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("transactionId", transactionId)
            .add("approvalMessage", approvalMessage)
            .toString();
    }
}
