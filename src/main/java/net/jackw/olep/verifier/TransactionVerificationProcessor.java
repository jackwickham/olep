package net.jackw.olep.verifier;

import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.message.transaction_request.DeliveryMessage;
import net.jackw.olep.message.transaction_request.PaymentMessage;
import net.jackw.olep.message.transaction_request.NewOrderMessage;
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
     * @param key   the key for the record
     * @param value the value for the record
     */
    @Override
    public void process(Long key, TransactionRequestMessage value) {
        System.out.printf("Processing transaction %d\n", key);
        if (value.body instanceof NewOrderMessage) {
            NewOrderMessage body = (NewOrderMessage) value.body;
            if (body.lines.stream().allMatch(line -> itemStore.containsKey(line.itemId))) {
                acceptTransaction(key, value);
            } else {
                rejectTransaction(key, value);
            }
        } else if (value.body instanceof PaymentMessage || value.body instanceof DeliveryMessage) {
            // These transactions can never fail (in theory)
            acceptTransaction(key, value);
        } else {
            // ???
            rejectTransaction(key, value);
        }
    }

    private void acceptTransaction(long id, TransactionRequestMessage transaction) {
        context.forward(id, transaction, To.child("accepted-transactions"));
        context.forward(id, new TransactionResultMessage(id, true), To.child("transaction-results"));
        System.out.println("Accepted a transaction of type " + transaction.body.getClass().getName());
    }

    private void rejectTransaction(long id, TransactionRequestMessage transaction) {
        context.forward(id, new TransactionResultMessage(id, false), To.child("transaction-results"));
        System.out.println("Rejected a messaged of type " + transaction.body.getClass().getName());
    }

    @Override
    public void close() {

    }
}
