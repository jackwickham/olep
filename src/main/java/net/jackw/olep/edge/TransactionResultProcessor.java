package net.jackw.olep.edge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import net.jackw.olep.message.transaction_result.ApprovalMessage;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;

public class TransactionResultProcessor {
    private final ObjectMapper objectMapper;

    public TransactionResultProcessor() {
        objectMapper = new ObjectMapper().registerModule(new GuavaModule());
    }

    /**
     * Decode a JSON representation of a TransactionResult, and update the corresponding pending transaction
     *
     * @param value The raw JSON data to process
     * @return Whether this transaction is now complete (no outstanding result messages)
     */
    public boolean process(boolean approvalMessage, byte[] value, PendingTransaction<?, ?> pendingTransaction) {
        try {
            if (approvalMessage) {
                // We just have the approval result, so update the pending transaction with this result
                ApprovalMessage result = objectMapper.readValue(value, ApprovalMessage.class);
                pendingTransaction.setAccepted(result.approved);
                return false;
            } else {
                // We have data about the transaction itself, so we need to update the result builder with the new info
                objectMapper.readerForUpdating(pendingTransaction.getTransactionResultBuilder()).readValue(value);
                // Then notify the transaction that we have done so
                return pendingTransaction.builderUpdated();
            }
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
