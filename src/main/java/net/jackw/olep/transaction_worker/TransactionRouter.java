package net.jackw.olep.transaction_worker;

import net.jackw.olep.message.transaction_request.NewOrderMessage;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Routes transactions to the correct transaction worker based on its type
 */
public class TransactionRouter implements Processor<Long, TransactionRequestMessage> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(Long key, TransactionRequestMessage value) {
        if (value.body instanceof NewOrderMessage) {
            context.forward(key, value.body);
        } else {
            // Nothing more we can do here...
            throw new IllegalArgumentException("Couldn't route transaction of type " + value.body.getClass().getName());
        }
    }

    @Override
    public void close() {

    }
}
