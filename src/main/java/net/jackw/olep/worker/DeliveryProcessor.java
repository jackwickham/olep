package net.jackw.olep.worker;

import net.jackw.olep.common.LogConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class DeliveryProcessor extends BaseTransactionProcessor<DeliveryRequest> {
    private NewOrdersStore newOrdersStore;
    private KeyValueStore<DistrictSpecificKey, CustomerMutable> customerMutableStore;

    private Metrics metrics;

    public DeliveryProcessor(Metrics metrics) {
        this.metrics = metrics;
    }


    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        this.newOrdersStore = new NewOrdersStore((KeyValueStore) context.getStateStore(KafkaConfig.NEW_ORDER_STORE));
        this.customerMutableStore = (KeyValueStore) context.getStateStore(KafkaConfig.CUSTOMER_MUTABLE_STORE);
    }

    @Override
    public void process(TransactionWarehouseKey key, DeliveryRequest value) {
        Timer timer = metrics.startTimer();
        log.debug(LogConfig.TRANSACTION_ID_MARKER, "Processing delivery transaction {}", key);
        final DeliveryResult.PartialResult results = new DeliveryResult.PartialResult();

        results.processedOrders = new HashMap<>(10);

        for (int i = 1; i <= 10; i++) {
            NewOrder order = newOrdersStore.poll(new WarehouseSpecificKey(i, value.warehouseId));
            if (order == null) {
                continue;
            }
            results.processedOrders.put(i, order.orderId);

            DistrictSpecificKey customerKey = new DistrictSpecificKey(order.customerId, i, value.warehouseId);
            CustomerMutable oldCustomer = customerMutableStore.get(customerKey);
            CustomerMutable newCustomer = new CustomerMutable(oldCustomer.balance.add(order.totalAmount), oldCustomer.data);
            customerMutableStore.put(customerKey, newCustomer);

            sendModification(key, new DeliveryModification(
                order.orderId, i, value.warehouseId, value.carrierId, value.deliveryDate, order.customerId, order.totalAmount
            ));
        }

        sendResults(key, results);
        metrics.recordDuration(DurationType.WORKER_DELIVERY, timer);
    }

    private static Logger log = LogManager.getLogger();
}
