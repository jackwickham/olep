package net.jackw.olep.edge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@SuppressWarnings("FutureReturnValueIgnored")
public class TransactionStatus<T> {
    private CompletableFuture<Void> writtenToLog;
    private CompletableFuture<Void> accepted;
    private CompletableFuture<T> complete;

    public TransactionStatus(
        CompletableFuture<?> writtenToLog,
        CompletableFuture<Void> accepted,
        CompletableFuture<T> complete
    ) {
        // Convert writtenToLog to a CompletableFuture<Void>
        this.writtenToLog = writtenToLog.thenApply(_a -> null);
        // Convert accepted to a future that only completes once writtenToLog and accepted have completed
        this.accepted = this.writtenToLog.thenCombine(accepted, (_a, _b) -> null);
        // Convert complete to a future that only completes once the new accepted, and complete, have completed
        this.complete = this.accepted.thenCombine(complete, (_a, completionResult) -> completionResult);
    }

    /**
     * Add a handler that will be called when the transaction has been delivered to the server.
     *
     * Handlers will be called in the order that they are added.
     *
     * @param handler The handler to be called when the transaction has been delivered
     */
    public void addDeliveredHandler(final Runnable handler) {
        writtenToLog.thenAccept(v -> handleExceptions(handler));
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
        writtenToLog.exceptionally(ex -> {
            handleExceptions(() -> handler.accept(ex));
            return null;
        });
    }

    /**
     * Add a callback that will be invoked when the transaction has been accepted by the system.
     *
     * When this callback is invoked, the effects may not have been applied to all of the views, but it is guaranteed
     * that they will be successfully applied at some point in the future.
     *
     * Handlers will be called in the order that they are registered.
     *
     * @param handler The callback to be invoked when the transaction is accepted
     */
    public void addAcceptedHandler(final Runnable handler) {
        accepted.thenAccept(_v -> handleExceptions(handler));
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
        accepted.exceptionally(throwable -> {
            handleExceptions(() -> handler.accept(throwable));
            return null;
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
        complete.thenAccept(v -> handleExceptions(() -> handler.accept(v)));
    }

    private void handleExceptions(Runnable method) {
        try {
            method.run();
        } catch (Throwable e) {
            log.error("Transaction event handler threw an exception", e);
        }
    }

    private static Logger log = LogManager.getLogger("TransactionStatus");
}
