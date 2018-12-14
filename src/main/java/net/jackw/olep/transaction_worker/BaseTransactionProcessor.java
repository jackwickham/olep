package net.jackw.olep.transaction_worker;

import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;
import net.jackw.olep.message.modification.ModificationMessage;
import net.jackw.olep.message.transaction_result.PartialTransactionResult;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

public abstract class BaseTransactionProcessor<K, V> implements Processor<K, V> {
    private int acceptedTransactionsPartitions;
    private ProcessorContext context;

    public BaseTransactionProcessor(int acceptedTransactionsPartitions) {
        this.acceptedTransactionsPartitions = acceptedTransactionsPartitions;
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void close() { }

    /**
     * Is this processor responsible for the given warehouse?
     *
     * @param warehouseId The warehouse to check responsibility for
     * @return Whether this processor is responsible for that warehouse
     */
    protected boolean isProcessorForWarehouse(int warehouseId, ProcessorContext context) {
        return warehouseId % acceptedTransactionsPartitions == context.partition();
    }

    /**
     * Send a modification message to Kafka
     *
     * @param transactionId The transaction ID
     * @param mod The modification
     */
    protected void sendModification(Long transactionId, ModificationMessage mod) {
        context.forward(transactionId, mod, To.child("modification-log"));
    }

    /**
     * Send transaction results to Kafka (and back to the application)
     *
     * @param transactionId The transaction ID
     * @param result The results to send
     */
    protected void sendResults(Long transactionId, PartialTransactionResult result) {
        context.forward(new TransactionResultKey(transactionId, false), result, To.child("transaction-results"));
    }
}
