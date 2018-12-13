package net.jackw.olep.transaction_worker;

import net.jackw.olep.common.LogConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.ModificationMessage;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.PartialTransactionResult;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class DeliveryProcessor extends BaseTransactionProcessor implements Processor<Long, DeliveryRequest> {
    private ProcessorContext context;
    private NewOrdersStore newOrdersStore;

    public DeliveryProcessor(int acceptedTransactionsPartitions) {
        super(acceptedTransactionsPartitions);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.newOrdersStore = new NewOrdersStore((KeyValueStore) context.getStateStore(KafkaConfig.NEW_ORDER_STORE));
    }

    @Override
    public void process(Long key, DeliveryRequest value) {
        log.debug(LogConfig.TRANSACTION_ID_MARKER, "Processing delivery transaction {}", key);
        final DeliveryResult.PartialResult results = new DeliveryResult.PartialResult();

        results.processedOrders = new HashMap<>(10);

        for (int i = 1; i <= 10; i++) {
            NewOrder order = newOrdersStore.poll(new WarehouseSpecificKey(i, value.warehouseId));
            if (order == null) {
                continue;
            }
            results.processedOrders.put(i, order.orderId);

            sendModification(key, new DeliveryModification(
                value.warehouseId, i, order.orderId, value.carrierId, order.customerId, order.totalAmount
            ));
        }

        sendResults(key, results);
    }

    @Override
    public void close() { }

    private void sendModification(Long transactionId, ModificationMessage mod) {
        context.forward(transactionId, mod, To.child("modification-log"));
    }

    private void sendResults(Long transactionId, PartialTransactionResult result) {
        context.forward(transactionId, new TransactionResultMessage(transactionId, result));
    }

    private static Logger log = LogManager.getLogger();
}
