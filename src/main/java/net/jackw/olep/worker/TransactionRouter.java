package net.jackw.olep.worker;

import net.jackw.olep.common.LRUSet;
import net.jackw.olep.common.BatchingLRUSet;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Routes transactions to the correct transaction worker based on its type
 */
public class TransactionRouter implements Processor<TransactionWarehouseKey, TransactionRequestMessage> {
    private ProcessorContext context;
    private final LRUSet<TransactionWarehouseKey> recentTransactions = new BatchingLRUSet<>(20000);

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(TransactionWarehouseKey key, TransactionRequestMessage value) {
        if (!recentTransactions.add(key)) {
            // Already processed this transaction for this warehouse
            return;
        }
        log.debug("Worker processing transaction {}: {}", key, value);
        if (value instanceof NewOrderRequest) {
            context.forward(key, value, To.child("new-order-processor"));
        } else if (value instanceof PaymentRequest) {
            context.forward(key, value, To.child("payment-processor"));
        } else if (value instanceof DeliveryRequest) {
            context.forward(key, value, To.child("delivery-processor"));
        } else {
            // Nothing more we can do here...
            throw new IllegalArgumentException("Couldn't route transaction of type " + value.getClass().getName());
        }
    }

    @Override
    public void close() {

    }

    private static Logger log = LogManager.getLogger();
}
