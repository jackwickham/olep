package net.jackw.olep.transaction_worker;

import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.message.modification.ModificationMessage;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_result.PartialTransactionResult;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

public abstract class BaseTransactionProcessor<V extends TransactionRequestMessage> implements Processor<TransactionWarehouseKey, V> {
    private ProcessorContext context;

    public BaseTransactionProcessor() { }

    @OverridingMethodsMustInvokeSuper
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void close() { }

    /**
     * Send a modification message to Kafka
     *
     * @param key The transaction key
     * @param mod The modification
     */
    protected void sendModification(TransactionWarehouseKey key, ModificationMessage mod) {
        context.forward(key.transactionId, mod, To.child(KafkaConfig.MODIFICATION_LOG));
    }

    /**
     * Send transaction results to Kafka (and back to the application)
     *
     * @param key The transaction key
     * @param result The results to send
     */
    protected void sendResults(TransactionWarehouseKey key, PartialTransactionResult result) {
        context.forward(
            new TransactionResultKey(key.transactionId, false), result,
            To.child(KafkaConfig.TRANSACTION_RESULT_TOPIC)
        );
    }
}
