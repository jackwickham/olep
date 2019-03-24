package net.jackw.olep.edge;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import net.jackw.olep.message.transaction_result.TransactionResultBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An internal representation of a pending transaction
 * @param <T> The class that will hold the result of this transaction
 * @param <B> A class for building T
 */
public class PendingTransaction<T extends TransactionResultMessage, B extends TransactionResultBuilder<T>> {
    private final long transactionId;
    private final B transactionResultBuilder;

    /**
     * The public transaction status object
     *
     * The CompletableFutures that represent the transaction's progress, {@link #writtenToLog}, {@link #accepted} and
     * {@link #complete}, are shared by this TransactionStatus, to allow changes to the state to be signalled to it.
     * These changes are then communicated to the consuming application through TransactionStatus's public API
     */
    private final TransactionStatus<T> transactionStatus;

    /**
     * A future that is completed successfully when the transaction has been successfully written to Kafka, and
     * completed exceptionally if the write to Kafka fails
     */
    private final SettableFuture<RecordMetadata> writtenToLog;

    /**
     * A future that is completed successfully when the transaction is accepted by the database, and therefore
     * guaranteed to be durable and to eventually succeed, and completed exceptionally if the transaction is rejected.
     *
     * If the transaction is not written to Kafka successfully, this future will never be completed, either successfully
     * or exceptionally.
     */
    private final SettableFuture<Void> accepted;

    /**
     * A future that is completed successfully with the result of this transaction if the transaction succeeds. It will
     * never complete exceptionally.
     *
     * If the transaction is not written to Kafka successfully, or the transaction is rejected by the database, this
     * future will never be completed (successfully or exceptionally)
     */
    private final SettableFuture<T> complete;

    /**
     * Construct a new pending transaction. The transaction will usually be about to be written to Kafka
     *
     * @param transactionId The unique identifier for this transaction
     * @param transactionResultBuilder The object to store incomplete results in
     */
    PendingTransaction(long transactionId, B transactionResultBuilder) {
        this.transactionId = transactionId;
        this.transactionResultBuilder = transactionResultBuilder;

        // Create the futures to keep track of the transaction status
        this.writtenToLog = SettableFuture.create();
        this.accepted = SettableFuture.create();
        this.complete = SettableFuture.create();

        this.transactionStatus = new TransactionStatus<>(transactionId, writtenToLog, accepted, complete);
    }

    /**
     * Get the object that partial results should be stored to
     */
    public B getTransactionResultBuilder() {
        return transactionResultBuilder;
    }

    /**
     * Get the ID of this transaction
     */
    public long getTransactionId() {
        return transactionId;
    }

    /**
     * Get the public transaction status object associated with this transaction
     */
    public TransactionStatus<T> getTransactionStatus() {
        return transactionStatus;
    }

    /**
     * Get the method that should be used as the callback when the transaction has been written to the log
     */
    public Callback getWrittenToLogCallback() {
        return (metadata, exception) -> {
            if (metadata != null) {
                this.writtenToLog.set(metadata);
            } else {
                this.writtenToLog.setException(exception);
            }
        };
    }

    /**
     * Notify this object that the associated transaction builder has been updated
     *
     * This method should be called after each TransactionResult message which contains results for this transaction
     * has been processed.
     *
     * @return Whether this transaction is now complete
     */
    public boolean builderUpdated() {
        if (transactionResultBuilder.canBuild()) {
            T result = transactionResultBuilder.build();
            log.debug("{} completed", transactionId);
            if (!complete.set(result)) {
                // We had already built this
                log.warn("{} - Builder shouldn't be updated after it has already been build", transactionId);
            }
            if (accepted.set(null)) {
                // We hadn't received the accepted message yet. As the transaction has completed, it must have been
                // accepted, so it was safe to mark it as such.
                // The actual accepted message will be discarded by the log consumer, because this transaction will be
                // added to the completed transactions cache
                log.warn("Received all results for {} before the acceptance message - marking accepted", transactionId);
            }
            return true;
        } else {
            log.debug("{} updated but not buildable", transactionId);
            return false;
        }
    }

    /**
     * Mark this transaction as accepted or rejected
     *
     * @param isAccepted Whether the transaction was accepted or rejected
     */
    public void setAccepted(boolean isAccepted) {
        if (isAccepted) {
            if (!accepted.set(null)) {
                log.warn("{} - Shouldn't set accepted after it has already been marked as accepted", transactionId);
            }
            log.debug("{} marked accepted", transactionId);
        } else {
            if (!accepted.setException(new TransactionRejectedException())) {
                log.warn("{} - Shouldn't set accepted after it has already been marked as accepted", transactionId);
            }
        }
    }

    @VisibleForTesting
    ListenableFuture<RecordMetadata> getWrittenToLogFuture() {
        return this.writtenToLog;
    }
    @VisibleForTesting
    ListenableFuture<Void> getAcceptedFuture() {
        return this.accepted;
    }
    @VisibleForTesting
    ListenableFuture<T> getCompleteFuture() {
        return complete;
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

    private static Logger log = LogManager.getLogger();
}
