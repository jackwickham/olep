package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class TransactionRequestMessage {
    public final long transactionId;
    public final TransactionRequestBody body;

    public TransactionRequestMessage(@JsonProperty("transactionId") long transactionId, @JsonProperty("body") TransactionRequestBody body) {
        this.transactionId = transactionId;
        this.body = body;
    }
}
