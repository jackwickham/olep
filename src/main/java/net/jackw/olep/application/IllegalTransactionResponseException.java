package net.jackw.olep.application;

public class IllegalTransactionResponseException extends RuntimeException {
    public IllegalTransactionResponseException(String message) {
        super(message);
    }
}
