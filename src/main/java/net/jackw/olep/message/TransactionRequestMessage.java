package net.jackw.olep.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Random;

@Immutable
public class TransactionRequestMessage {
    public final long transactionId;
    public final TransactionRequestBody body;

    public TransactionRequestMessage(@JsonProperty("transactionId") long transactionId, @JsonProperty("body") TransactionRequestBody body) {
        this.transactionId = transactionId;
        this.body = body;
    }
}
