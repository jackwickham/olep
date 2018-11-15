package net.jackw.olep.edge;

import net.jackw.olep.edge.transaction_result.TransactionResult;
import net.jackw.olep.edge.transaction_result.TransactionResultBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;

public class PendingTransaction<T extends TransactionResult, B extends TransactionResultBuilder<T>> {
    public final long transactionId;
    public final TransactionStatus<T> transactionStatus;
    private final CompletableFuture<Void> writtenToLog;
    private final CompletableFuture<Void> accepted;
    private final CompletableFuture<T> complete;
    private final B transactionResultBuilder;

    public final Callback writtenToLogCallback;

    public PendingTransaction(long transactionId, Class<B> resultBuilderClass) {
        this.transactionId = transactionId;
        try {
            this.transactionResultBuilder = resultBuilderClass.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("TransactionResultBuilder must have a public constructor with no arguments", e);
        }

        this.writtenToLog = new CompletableFuture<>();
        this.accepted = new CompletableFuture<>();
        this.complete = new CompletableFuture<>();
        this.transactionStatus = new TransactionStatus<>(writtenToLog, accepted, complete);

        writtenToLogCallback = ((metadata, exception) -> {
            if (metadata != null) {
                this.writtenToLog.complete(null);
            } else {
                this.writtenToLog.completeExceptionally(exception);
            }
        });
    }

    public B getTransactionResultBuilder() {
        return transactionResultBuilder;
    }

    public void builderUpdated() {
        if (transactionResultBuilder.canBuild()) {
            T result = transactionResultBuilder.build();
            if (!complete.complete(result)) {
                // We had already built this
                log.warn("Builder shouldn't be updated after it has already been build");
            }
            System.out.println("completed");
        } else {
            System.out.println("updated but not buildable");
        }
    }

    /**
     * Mark this transaction as accepted or rejected
     * @param isAccepted Whether the transaction was accepted or rejected
     */
    public void setAccepted(boolean isAccepted) {
        if (isAccepted) {
            if (!accepted.complete(null)) {
                log.warn("Shouldn't set accepted after it has already been marked as accepted");
            }
            System.out.println("Marked accepted");
        } else {
            if (!accepted.completeExceptionally(new Exception())) {
                log.warn("Shouldn't set accepted after it has already been marked as accepted");
            }
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(transactionId);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PendingTransaction)) {
            return false;
        }
        PendingTransaction other = (PendingTransaction) obj;
        return transactionId == other.transactionId;
    }

    private static Logger log = LogManager.getLogger("PendingTransaction");
}
