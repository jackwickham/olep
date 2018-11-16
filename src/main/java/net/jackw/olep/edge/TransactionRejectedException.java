package net.jackw.olep.edge;

public class TransactionRejectedException extends Exception {
    public TransactionRejectedException() {
        super();
    }

    public TransactionRejectedException(String message) {
        super(message);
    }
}
