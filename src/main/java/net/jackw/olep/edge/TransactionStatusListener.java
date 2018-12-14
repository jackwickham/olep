package net.jackw.olep.edge;

import net.jackw.olep.message.transaction_result.TransactionResultMessage;

public interface TransactionStatusListener<T extends TransactionResultMessage> {
    /**
     * Callback when the transaction request has been committed to the log
     *
     * This method doesn't mean that the transaction has been accepted, but it does mean that eventually it will be
     * considered.
     */
    default void deliveredHandler() { }

    /**
     * Callback when the transaction request was not successfully committed to the log
     *
     * This is usually because of a problem with the log itself
     *
     * @param t The exception that caused this failure
     */
    default void deliveryFailedHandler(Throwable t) { }

    /**
     * Callback when the transaction has been accepted by the database
     *
     * When this method is called, the transaction is guaranteed to eventually be successfully written to the database.
     * However, it may not have been materialised in the views yet, and the results of the transaction may not yet be
     * available.
     */
    default void acceptedHandler() { }

    /**
     * Callback when the transaction was unable to complete successfully.
     *
     * This method will be called if the transaction fails to commit to disk, or if the database determines that it
     * contains a consistency violation and cannot proceed.
     *
     * Consistency violations have exceptions of type {@link TransactionRejectedException}.
     *
     * @param t The exception that caused this failure
     */
    default void rejectedHandler(Throwable t) { }

    /**
     * Callback when the transaction has been successfully completed and the results are available.
     *
     * When this method is called, the transaction may not have been materialised in the views, but it is guaranteed
     * that the modifications will be performed eventually.
     *
     * @param result The results from this transaction
     */
    default void completeHandler(T result) { }
}
