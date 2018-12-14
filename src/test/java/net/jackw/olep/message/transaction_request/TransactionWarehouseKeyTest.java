package net.jackw.olep.message.transaction_request;

import org.junit.Test;

import static org.junit.Assert.*;

public class TransactionWarehouseKeyTest {
    @Test
    public void testSerializeDeserializeWorks() {
        TransactionWarehouseKey.KeySerializer serializer = new TransactionWarehouseKey.KeySerializer();
        TransactionWarehouseKey.KeyDeserializer deserializer = new TransactionWarehouseKey.KeyDeserializer();
        TransactionWarehouseKey key = new TransactionWarehouseKey(Long.MIN_VALUE, Integer.MAX_VALUE);

        byte[] data = serializer.serialize("topic", key);
        TransactionWarehouseKey result = deserializer.deserialize("topic", data);

        assertEquals(Long.MIN_VALUE, result.transactionId);
        assertEquals(Integer.MAX_VALUE, result.warehouseId);
    }
}
