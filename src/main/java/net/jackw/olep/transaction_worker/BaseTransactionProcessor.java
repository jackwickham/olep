package net.jackw.olep.transaction_worker;

import org.apache.kafka.streams.processor.ProcessorContext;

public abstract class BaseTransactionProcessor {
    private int acceptedTransactionsPartitions;

    public BaseTransactionProcessor(int acceptedTransactionsPartitions) {
        this.acceptedTransactionsPartitions = acceptedTransactionsPartitions;
    }

    /**
     * Is this processor responsible for the given warehouse?
     *
     * @param warehouseId The warehouse to check responsibility for
     * @return Whether this processor is responsible for that warehouse
     */
    protected boolean isProcessorForWarehouse(int warehouseId, ProcessorContext context) {
        return warehouseId % acceptedTransactionsPartitions == context.partition();
    }
}
