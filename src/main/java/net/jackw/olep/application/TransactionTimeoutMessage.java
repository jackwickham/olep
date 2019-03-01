package net.jackw.olep.application;

public class TransactionTimeoutMessage {
    private long transactionId;
    private TransactionType transactionType;

    public TransactionTimeoutMessage(long transactionId, TransactionType transactionType) {
        this.transactionId = transactionId;
        this.transactionType = transactionType;
    }

    public TransactionTimeoutMessage(TransactionType transactionType) {
        this.transactionId = -1;
        this.transactionType = transactionType;
    }

    @Override
    public String toString() {
        return String.format("Transaction #%d of type %s", transactionId, transactionType);
    }
}
