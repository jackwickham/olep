package net.jackw.olep.edge;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * The result of a transaction, which may potentially still be pending.
 *
 * To receive the result of the transaction, handlers should be passed to the relevant attachment method. These handlers
 * will be called in the order that they become applicable, then in the order that they were attached within that.
 *
 * If the result is available or the property has already been satisfied when the handler is attached, it will be called
 * synchronously, before the attachment method returns (from the same thread). Otherwise, it will be called once the
 * result becomes available or the property is satisfied, potentially from a different thread.
 *
 * @param <T> The type of the transaction result
 */
@SuppressWarnings("FutureReturnValueIgnored")
public class TransactionStatus<T extends TransactionResultMessage> {
    private long transactionId;
    private ListenableFuture<Void> writtenToLog;
    private ListenableFuture<Void> accepted;
    private ListenableFuture<T> complete;

    /**
     * Create the status for a new transaction
     *
     * @param writtenToLog A future that will complete successfully when the transaction has been written to the event
     *                     log, and exceptionally if the transaction is unable to be written to the event log
     * @param accepted A future that will complete successfully when the transaction is accepted by the database system,
     *                 and exceptionally if the transaction is rejected by the database system
     * @param complete A future that will complete successfully when the results of the transaction have been retrieved
     *                 from the database system, and will never complete exceptionally.
     */
    @SuppressWarnings("unchecked")
    TransactionStatus(
        long transactionId,
        ListenableFuture<?> writtenToLog,
        ListenableFuture<Void> accepted,
        ListenableFuture<T> complete
    ) {
        this.transactionId = transactionId;
        // Convert writtenToLog to a CompletableFuture<Void>
        this.writtenToLog = Futures.transform(writtenToLog, _a -> null, MoreExecutors.directExecutor());
        // Convert accepted to a future that only completes once writtenToLog and accepted have completed
        // Using .thenCompose so if writtenToLog fails, the resulting future will immediately fail too - .thenCombine
        // waits for both futures to complete before failing
        this.accepted = Futures.transform(Futures.allAsList(writtenToLog, accepted), _r -> null, MoreExecutors.directExecutor());
        // Convert complete to a future that only completes once the new accepted, and complete, have completed
        this.complete = Futures.transform(
            Futures.allAsList(writtenToLog, accepted, complete),
            results -> (T) results.get(2),
            MoreExecutors.directExecutor()
        );
    }

    /**
     * Register a {@link TransactionStatusListener} to handle all of the potential events
     *
     * @param handlers The TransactionStatusListener that contains all of the handlers
     */
    public void register(TransactionStatusListener<? super T> handlers) {
        Futures.addCallback(
            writtenToLog,
            new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable Void result) {
                    try {
                        handlers.deliveredHandler();
                    } catch (Exception e) {
                        log.error("Transaction event handler threw an exception", e);
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    try {
                        handlers.deliveryFailedHandler(t);
                    } catch (Exception e) {
                        log.error("Transaction event handler threw an exception", e);
                    }
                }
            },
            MoreExecutors.directExecutor()
        );

        Futures.addCallback(
            accepted,
            new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable Void result) {
                    try {
                        handlers.acceptedHandler();
                    } catch (Exception e) {
                        log.error("Transaction event handler threw an exception", e);
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    try {
                        handlers.rejectedHandler(t);
                    } catch (Exception e) {
                        log.error("Transaction event handler threw an exception", e);
                    }
                }
            },
            MoreExecutors.directExecutor()
        );

        Futures.addCallback(
            complete,
            new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable T result) {
                    try {
                        handlers.completeHandler(result);
                    } catch (Exception e) {
                        log.error("Transaction event handler threw an exception", e);
                    }
                }

                @Override
                public void onFailure(Throwable t) { }
            },
            MoreExecutors.directExecutor()
        );
    }

    /**
     * Get a future that resolves when the transaction has successfully been persisted, and will be processed at some
     * point in the future
     */
    public ListenableFuture<Void> getDeliveryFuture() {
        return writtenToLog;
    }

    /**
     * Get a future that resolves when the transaction is guaranteed to have been accepted, although the results may
     * not be available yet
     */
    public ListenableFuture<Void> getAcceptedFuture() {
        return accepted;
    }

    /**
     * Get a future that resolves with the transaction results when they are available
     */
    public ListenableFuture<T> getCompleteFuture() {
        return complete;
    }

    /**
     * Add a handler that will be called when the transaction has been delivered to the server.
     *
     * @param handler The handler to be called when the transaction has been delivered
     */
    public void addDeliveredHandler(final Runnable handler) {
        register(new TransactionStatusListener<>() {
            @Override
            public void deliveredHandler() {
                handler.run();
            }
        });
    }

    /**
     * Add a handler that will be called if the transaction is unable to be delivered to the server.
     *
     * The transaction is guaranteed to not be performed if this callback is invoked.
     *
     * The callback will be invoked with the exception that caused the failure.
     *
     * @param handler The callback to invoke if the transaction failed
     */
    public void addDeliveryFailedHandler(final Consumer<Throwable> handler) {
        register(new TransactionStatusListener<>() {
            @Override
            public void deliveryFailedHandler(Throwable t) {
                handler.accept(t);
            }
        });
    }

    /**
     * Add a callback that will be invoked when the transaction has been accepted by the system.
     *
     * When this callback is invoked, the effects may not have been applied to all of the views, but it is guaranteed
     * that they will be successfully applied at some point in the future.
     *
     * @param handler The callback to be invoked when the transaction is accepted
     */
    public void addAcceptedHandler(final Runnable handler) {
        register(new TransactionStatusListener<>() {
            @Override
            public void acceptedHandler() {
                handler.run();
            }
        });
    }

    /**
     * Add a callback that will be invoked if the transaction is not completed successfully.
     *
     * If the transaction is not successfully delivered to the disk, all handlers registered with {@link
     * #addDeliveryFailedHandler(Consumer)} and all handlers registered with this method will be invoked.
     *
     * If the application is only interested in transactions that were explicitly rejected by the database, they should
     * filter the calls to just those where the argument has type {@link TransactionRejectedException}.
     *
     * @param handler The callback to be invoked with the error that occurred
     */
    public void addRejectedHandler(final Consumer<Throwable> handler) {
        register(new TransactionStatusListener<>() {
            @Override
            public void rejectedHandler(Throwable t) {
                handler.accept(t);
            }
        });
    }

    /**
     * Add a callback that will be called with the transaction's result on successful completion of the transaction.
     *
     * When this callback is invoked, the transaction's effects are not guaranteed to have shown up in the database's
     * views, but it is guaranteed that they will at some point in the future.
     *
     * @param handler The handler to receive the transaction results when they are available
     */
    public void addCompleteHandler(final Consumer<T> handler) {
        register(new TransactionStatusListener<>() {
            @Override
            public void completeHandler(T result) {
                handler.accept(result);
            }
        });
    }

    /**
     * Get the internal ID for this transaction
     */
    public long getTransactionId() {
        return transactionId;
    }

    private static Logger log = LogManager.getLogger();
}
