package net.jackw.olep.edge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Map;

public class TransactionResultProcessor {
    private final ObjectMapper objectMapper;

    public TransactionResultProcessor(Map<Long, PendingTransaction<?, ?>> pendingTransactionMap) {
        objectMapper = new ObjectMapper().registerModule(new GuavaModule());
        TransactionResultJsonDeserializer jsonDeserializer = new TransactionResultJsonDeserializer(
            pendingTransactionMap, objectMapper
        );

        // Register the transaction result deserializer (which deserializes transaction results to PendingTransactions)
        SimpleModule module = new SimpleModule();
        module.addDeserializer(PendingTransaction.class, jsonDeserializer);
        this.objectMapper.registerModule(module);
    }

    /**
     * Decode a JSON representation of a TransactionResult, and update the corresponding pending transaction
     *
     * @param data The raw JSON data to process
     */
    public void process(byte[] data) {
        try {
            objectMapper.readValue(data, PendingTransaction.class);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
