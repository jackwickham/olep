package net.jackw.olep.application;

public class TransactionTimeoutException extends RuntimeException {
    public TransactionTimeoutException(String message) {
        super(message);
    }
}
