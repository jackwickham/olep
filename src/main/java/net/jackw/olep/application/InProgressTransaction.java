package net.jackw.olep.application;

public class InProgressTransaction {
    private long transactionId;
    private TransactionType transactionType;

    public InProgressTransaction(long transactionId, TransactionType transactionType) {
        this.transactionId = transactionId;
        this.transactionType = transactionType;
    }

    @Override
    public String toString() {
        return String.format("Transaction #%d of type %s", transactionId, transactionType);
    }
}
