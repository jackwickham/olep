package net.jackw.olep.transaction_worker;

import net.jackw.olep.common.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Routes transactions to the correct transaction worker based on its type
 */
public class TransactionRouter implements Processor<TransactionWarehouseKey, TransactionRequestMessage> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(TransactionWarehouseKey key, TransactionRequestMessage value) {
        if (value instanceof NewOrderRequest) {
            context.forward(key.transactionId, value);
        } else {
            // Nothing more we can do here...
            throw new IllegalArgumentException("Couldn't route transaction of type " + value.getClass().getName());
        }
    }

    @Override
    public void close() {

    }
}
