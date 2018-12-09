package net.jackw.olep.message.transaction_result;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import java.util.Map;

@JsonPropertyOrder({"transactionId", "approved", "results"})
public class TransactionResultMessage {
    public final long transactionId;
    public final Boolean approved;
    public final PartialTransactionResult results;

    public TransactionResultMessage(
        @JsonProperty("transactionId") long transactionId,
        @JsonProperty("approved") Boolean approved,
        @JsonProperty("results") PartialTransactionResult results
    ) {
        this.transactionId = transactionId;
        this.approved = approved;
        this.results = results;
    }

    public TransactionResultMessage(
        long transactionId,
        PartialTransactionResult results
    ) {
        this(transactionId, null, results);
    }

    public TransactionResultMessage(
        long transactionId,
        Boolean approved
    ) {
        this(transactionId, approved, null);
    }
}
