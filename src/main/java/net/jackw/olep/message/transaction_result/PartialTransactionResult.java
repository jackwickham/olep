package net.jackw.olep.message.transaction_result;

/**
 * Part of a transaction result. The transaction workers will set properties in the classes that implement this
 * interface, which can then be serialised and sent over the network back to the edge. A TransactionResultBuilder will
 * then use those values to create the complete TransactionResult
 */
public interface PartialTransactionResult {
}
