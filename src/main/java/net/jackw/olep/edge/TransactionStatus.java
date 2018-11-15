package net.jackw.olep.edge;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

public class TransactionStatus<T> {
    private CompletableFuture<?> writtenToLog;
    private CompletableFuture<Void> accepted;
    private CompletableFuture<T> complete;

    public TransactionStatus(
        CompletableFuture<?> writtenToLog,
        CompletableFuture<Void> accepted,
        CompletableFuture<T> complete
    ) {
        this.writtenToLog = writtenToLog;
        this.accepted = accepted;
        this.complete = complete;
    }

    /**
     * Block until the transaction has been delivered to the server
     * @throws InterruptedException
     */
    public void waitUntilDelivered() throws InterruptedException {
        try {
            writtenToLog.get();
        } catch (ExecutionException e) {
            // This shouldn't be possible
            // TODO: ^ is not correct
            throw new RuntimeException(e);
        }
    }

    /**
     * Block until the transaction has been delivered to the server
     * @param timeout
     * @param timeUnit
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void waitUntilDelivered(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        try {
            writtenToLog.get(timeout, timeUnit);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add a handler that will be called when the transaction has been delivered to the server
     * @param handler
     */
    public void addDeliveredHandler(final Consumer<Void> handler) {
        writtenToLog.thenAccept(v -> handler.accept(null));
    }

    public void addAcceptedHandler(final Consumer<Void> handler) {
        accepted.thenAccept(v -> handler.accept(null));
    }

    /**
     * @todo This should possibly also provide a reason
     * @param handler
     */
    public void addRejectedHandler(final Consumer<Void> handler) {
        accepted.exceptionally(throwable -> {
            handler.accept(null);
            return null;
        });
    }

    public void addCompleteHandler(final Consumer<T> handler) {
        complete.thenAccept(handler);
    }
}
