package net.jackw.olep.verifier;

import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

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
        System.out.printf("Processing transaction %d\n", key);
        if (message instanceof NewOrderRequest) {
            NewOrderRequest body = (NewOrderRequest) message;
            if (body.lines.stream().allMatch(line -> itemStore.containsKey(line.itemId))) {
                acceptTransaction(key, message);
            } else {
                rejectTransaction(key, message);
            }
        } else if (message instanceof PaymentRequest || message instanceof DeliveryRequest) {
            // These transactions can never fail (in theory)
            acceptTransaction(key, message);
        } else {
            // ??? Don't recognise this transaction, so reject it
            rejectTransaction(key, message);
        }
    }

    private void acceptTransaction(long id, TransactionRequestMessage transaction) {
        context.forward(id, transaction, To.child("accepted-transactions"));
        context.forward(id, new TransactionResultMessage(id, true), To.child("transaction-results"));
        System.out.println("Accepted a transaction of type " + transaction.getClass().getName());
    }

    private void rejectTransaction(long id, TransactionRequestMessage transaction) {
        context.forward(id, new TransactionResultMessage(id, false), To.child("transaction-results"));
        System.out.println("Rejected a messaged of type " + transaction.getClass().getName());
    }

    @Override
    public void close() {

    }
}
