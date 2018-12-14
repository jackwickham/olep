package net.jackw.olep.verifier;

import net.jackw.olep.common.LogConfig;
import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.TransactionWarehouseKey;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_result.ApprovalMessage;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class TransactionVerificationProcessor implements Processor<Long, TransactionRequestMessage> {
    private ProcessorContext context;
    private final SharedKeyValueStore<Integer, Item> itemStore;

    public TransactionVerificationProcessor(SharedKeyValueStore<Integer, Item> itemStore) {
        this.itemStore = itemStore;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    /**
     * Process the record with the given key and value.
     *
     * @param key     the key for the record
     * @param message the value for the record
     */
    @Override
    public void process(Long key, TransactionRequestMessage message) {
        if (message instanceof NewOrderRequest) {
            NewOrderRequest body = (NewOrderRequest) message;
            if (body.lines.stream().allMatch(line -> itemStore.containsKey(line.itemId))) {
                acceptTransaction(key, message, message.getWorkerWarehouses());
            } else {
                rejectTransaction(key, message);
            }
        } else if (message instanceof PaymentRequest || message instanceof DeliveryRequest) {
            // These transactions can never fail (in theory)
            acceptTransaction(key, message, message.getWorkerWarehouses());
        } else {
            // ??? Don't recognise this transaction, so reject it
            log.error(LogConfig.TRANSACTION_PROCESSING_MARKER, "Attempted to process unrecognised transaction with type {}", message.getClass().getName());
            rejectTransaction(key, message);
        }
    }

    private void acceptTransaction(long id, TransactionRequestMessage transaction, Set<Integer> warehouses) {
        // Forward the transaction to each of the relevant warehouses
        log.debug(LogConfig.TRANSACTION_PROCESSING_MARKER, "Forwarding {} to warehouses {}", id, warehouses);
        for (int warehouse : warehouses) {
            context.forward(new TransactionWarehouseKey(id, warehouse), transaction, To.child("accepted-transactions"));
        }
        // Then write the acceptance message to the result log
        context.forward(new TransactionResultKey(id, true), new ApprovalMessage(true), To.child("transaction-results"));
        log.debug(LogConfig.TRANSACTION_DONE_MARKER, "Accepted transaction {} of type {}", id, transaction.getClass().getSimpleName());
    }

    private void rejectTransaction(long id, TransactionRequestMessage transaction) {
        context.forward(new TransactionResultKey(id, true), new ApprovalMessage(false), To.child("transaction-results"));
        log.debug(LogConfig.TRANSACTION_DONE_MARKER, "Rejected transaction {} of type {}", id, transaction.getClass().getSimpleName());
    }

    @Override
    public void close() {

    }

    private static Logger log = LogManager.getLogger();
}
