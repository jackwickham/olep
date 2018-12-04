package net.jackw.olep.verifier;

import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.edge.transaction_result.NewOrderResult;
import net.jackw.olep.message.NewOrderMessage;
import net.jackw.olep.message.TestMessage;
import net.jackw.olep.message.TransactionRequestMessage;
import net.jackw.olep.message.TransactionResultMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
        if (value.body instanceof TestMessage) {
            TestMessage body = (TestMessage) value.body;
            if (itemStore.contains(body.item)) {
                acceptTransaction(key, value);
            } else {
                rejectTransaction(key, value);
            }
        } else if (value.body instanceof NewOrderMessage) {
            NewOrderMessage body = (NewOrderMessage) value.body;
            if (body.lines.stream().allMatch(line -> itemStore.contains(line.itemId))) {
                acceptTransaction(key, value);
                context.forward(key, new TransactionResultMessage(key, Map.of(
                    "warehouseId", body.warehouseId,
                    "districtId", body.districtId,
                    "customerId", body.customerId,
                    "orderDate", new Date().getTime(),
                    "orderId", 500,
                    "customerSurname", "FOOBAR",
                    "credit", Credit.GC,
                    "lineCount", 1
                )), To.child("transaction-results"));
                context.forward(key, new TransactionResultMessage(key, Map.of(
                    "discount", new BigDecimal("0.02"),
                    "warehouseTax", new BigDecimal("0.3"),
                    "districtTax", new BigDecimal("0.14"),
                    "lines", List.of(new NewOrderResult.OrderLineResult(1, 2, "foobaritem", 1, 54, new BigDecimal("10.54"), new BigDecimal("10.54")))
                )), To.child("transaction-results"));
            } else {
                rejectTransaction(key, value);
            }

        } else {
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
