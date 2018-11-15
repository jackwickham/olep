package net.jackw.olep.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

@Immutable
@JsonPropertyOrder({"transactionId", "approved", "results"})
public class TransactionResultMessage {
    public final long transactionId;
    public final Boolean approved;
    @SuppressWarnings("Immutable")
    public final ImmutableMap<String, ?> results;

    public TransactionResultMessage(
        @JsonProperty("transactionId") long transactionId,
        @JsonProperty("approved") Boolean approved,
        @JsonProperty("results") ImmutableMap<String, ?> results
    ) {
        this.transactionId = transactionId;
        this.approved = approved;
        this.results = results;
    }

    public TransactionResultMessage(
        long transactionId,
        ImmutableMap<String, ?> results
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
