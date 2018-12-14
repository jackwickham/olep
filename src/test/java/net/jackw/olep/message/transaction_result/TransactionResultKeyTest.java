package net.jackw.olep.message.transaction_result;

import org.junit.Test;

import static org.junit.Assert.*;

public class TransactionResultKeyTest {
    @Test
    public void testSerializeDeserializeWorks() {
        TransactionResultKey.ResultKeySerializer serializer = new TransactionResultKey.ResultKeySerializer();
        TransactionResultKey.ResultKeyDeserializer deserializer = new TransactionResultKey.ResultKeyDeserializer();
        TransactionResultKey key = new TransactionResultKey(Long.MIN_VALUE, false);

        byte[] data = serializer.serialize("topic", key);
        TransactionResultKey result = deserializer.deserialize("topic", data);

        assertEquals(Long.MIN_VALUE, result.transactionId);
        assertFalse(result.approvalMessage);
    }

    @Test
    public void testTransactionIdParsedCorrectly() {
        long transactionId = 0xFFFFFFFF_80000001L;
        TransactionResultKey key = new TransactionResultKey(transactionId, true);
        assertEquals(-1, key.getTransactionSerialId());
        assertEquals(Integer.MIN_VALUE + 1, key.getConnectionId());
    }
}
