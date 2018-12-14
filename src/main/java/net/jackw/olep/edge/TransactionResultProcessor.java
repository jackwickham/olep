package net.jackw.olep.edge;

import com.fasterxml.jackson.core.type.TypeReference;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.message.transaction_result.ApprovalMessage;
import net.jackw.olep.message.transaction_result.TransactionResultBuilder;

public class TransactionResultProcessor {
    private final JsonDeserializer<ApprovalMessage> approvalDeserializer;
    private final JsonDeserializer<TransactionResultBuilder<?>> transactionResultDeserializer;

    public TransactionResultProcessor() {
        approvalDeserializer = new JsonDeserializer<>(ApprovalMessage.class);
        transactionResultDeserializer = new JsonDeserializer<>(new TypeReference<>() {});
    }

    /**
     * Decode a JSON representation of a TransactionResult, and update the corresponding pending transaction
     *
     * @param value The raw JSON data to process
     * @return Whether this transaction is now complete (no outstanding result messages)
     */
    public boolean process(boolean approvalMessage, byte[] value, PendingTransaction<?, ?> pendingTransaction) {
        if (approvalMessage) {
            // We just have the approval result, so update the pending transaction with this result
            ApprovalMessage result = approvalDeserializer.deserialize(value);
            pendingTransaction.setAccepted(result.approved);
            return false;
        } else {
            // We have data about the transaction itself, so we need to update the result builder with the new info
            transactionResultDeserializer.deserialize(value, pendingTransaction.getTransactionResultBuilder());
            // Then notify the transaction that we have done so
            return pendingTransaction.builderUpdated();
        }
    }
}
