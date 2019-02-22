package net.jackw.olep.worker;

import net.jackw.olep.common.LogConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.store.SharedKeyValueStore;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.message.modification.RemoteStockModification;
import net.jackw.olep.message.modification.OrderLineModification;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.OrderLineResult;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
import net.jackw.olep.utils.RandomDataGenerator;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;

/**
 * Process a New-Order transaction that affects this worker
 */
public class NewOrderProcessor extends BaseTransactionProcessor<NewOrderRequest> {
    private LocalStore<WarehouseSpecificKey, Integer> nextOrderIdStore;
    private LocalStore<WarehouseSpecificKey, Integer> stockQuantityStore;
    private NewOrdersStore newOrdersStore;

    private final SharedKeyValueStore<Integer, Item> itemStore;
    private final SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore;
    private final SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore;
    private final SharedKeyValueStore<DistrictSpecificKey, CustomerShared> customerImmutableStore;
    private final SharedKeyValueStore<WarehouseSpecificKey, StockShared> stockImmutableStore;

    private final Metrics metrics;

    private final RandomDataGenerator rand = new RandomDataGenerator();

    public NewOrderProcessor(
        SharedKeyValueStore<Integer, Item> itemStore,
        SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore,
        SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore,
        SharedKeyValueStore<DistrictSpecificKey, CustomerShared> customerImmutableStore,
        SharedKeyValueStore<WarehouseSpecificKey, StockShared> stockImmutableStore,
        Metrics metrics
    ) {
        this.itemStore = itemStore;
        this.warehouseImmutableStore = warehouseImmutableStore;
        this.districtImmutableStore = districtImmutableStore;
        this.customerImmutableStore = customerImmutableStore;
        this.stockImmutableStore = stockImmutableStore;
        this.metrics = metrics;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);

        this.nextOrderIdStore = new LocalStore<WarehouseSpecificKey, Integer>(
            (KeyValueStore) context.getStateStore(KafkaConfig.DISTRICT_NEXT_ORDER_ID_STORE), 1
        );
        this.stockQuantityStore = new LocalStore<WarehouseSpecificKey, Integer>(
            (KeyValueStore) context.getStateStore(KafkaConfig.STOCK_QUANTITY_STORE),
            // Default value is S_QUANTITY random within [10 .. 100]
            () -> rand.uniform(10, 100)
        );
        this.newOrdersStore = new NewOrdersStore((KeyValueStore) context.getStateStore(KafkaConfig.NEW_ORDER_STORE));
    }

    @Override
    public void process(TransactionWarehouseKey key, NewOrderRequest value) {
        Timer timer = metrics.startTimer();
        log.debug(LogConfig.TRANSACTION_ID_MARKER, "Processing new-order transaction {}", key);
        boolean local = false;
        try {
            final NewOrderResult.PartialResult results = new NewOrderResult.PartialResult();
            WarehouseSpecificKey districtKey = new WarehouseSpecificKey(value.districtId, value.warehouseId);

            NewOrderModificationBuilder orderBuilder = null;

            if (key.warehouseId == value.warehouseId) {
                // We need to process it for the home warehouse
                local = true;

                // Load values from the immutable stores
                WarehouseShared warehouse = warehouseImmutableStore.getBlocking(value.warehouseId);
                DistrictShared district = districtImmutableStore.getBlocking(districtKey);
                CustomerShared customer = customerImmutableStore.getBlocking(
                    new DistrictSpecificKey(value.customerId, value.districtId, value.warehouseId)
                );

                int orderId = nextOrderIdStore.get(districtKey);
                nextOrderIdStore.put(districtKey, orderId + 1);

                // Send the client the results general results
                results.orderId = orderId;
                results.customerLastName = customer.lastName;
                results.credit = customer.credit;
                results.discount = customer.discount;
                results.warehouseTax = warehouse.tax;
                results.districtTax = district.tax;

                // Create the order builder, so we can put the line items into it
                orderBuilder = new NewOrderModificationBuilder(
                    orderId, value.districtId, value.warehouseId, value.customerId, value.date
                );
            }

            int nextLineNumber = 0;
            for (NewOrderRequest.OrderLine line : value.lines) {
                int lineNumber = nextLineNumber++;

                Item item = itemStore.getBlocking(line.itemId);

                WarehouseSpecificKey dispatchingWarehouseStockKey = new WarehouseSpecificKey(line.itemId, line.supplyingWarehouseId);

                // Stock can never actually be < 0, so use this as a placeholder value
                // Alternative is null or Option, but both are ugly in the JVM
                int stockQuantity = -1;

                if (key.warehouseId == line.supplyingWarehouseId) {
                    // The line comes from a warehouse that we are responsible for, so decrease the stock level
                    stockQuantity = stockQuantityStore.get(dispatchingWarehouseStockKey);
                    int excessStock = stockQuantity - line.quantity;
                    if (excessStock < 10) {
                        stockQuantity += 91;
                    }
                    stockQuantity -= line.quantity;
                    stockQuantityStore.put(dispatchingWarehouseStockKey, stockQuantity);

                    results.addLine(lineNumber, new OrderLineResult.PartialResult(stockQuantity));
                }

                if (local) {
                    // If this is a local order, we need to give the user most of the results, and send a modification
                    // message with the order details

                    // Load the stock data for this line
                    StockShared stockShared = stockImmutableStore.getBlocking(dispatchingWarehouseStockKey);
                    if (stockQuantity == -1) {
                        // dispatchingWarehouseStockKey is incorrect here, because we want the home warehouse level
                        stockQuantity = stockQuantityStore.get(new WarehouseSpecificKey(line.itemId, value.warehouseId));
                    }

                    // Add the order line to the generated order
                    BigDecimal lineAmount = item.price.multiply(new BigDecimal(line.quantity));
                    String distInfo = stockShared.getDistrictInfo(value.districtId);
                    OrderLineModification orderLine = new OrderLineModification(
                        lineNumber, line.itemId, line.supplyingWarehouseId, line.quantity, lineAmount, stockQuantity,
                        distInfo
                    );

                    orderBuilder.addOrderLine(orderLine);

                    results.addLine(lineNumber, new OrderLineResult.PartialResult(
                        item.name, item.price, lineAmount
                    ));
                } else if (line.supplyingWarehouseId == key.warehouseId) {
                    // It's remote, and we are the responsible warehouse, so we just need to update the views with
                    // details about the new stock level
                    sendModification(key, new RemoteStockModification(item.id, line.supplyingWarehouseId, stockQuantity), (short) lineNumber);
                }
            }

            if (local) {
                newOrdersStore.add(districtKey, orderBuilder.buildNewOrder());
                // Forward the transaction to the modification log
                sendModification(key, orderBuilder.build(), (short) 20);
            }

            sendResults(key, results);
        } catch (InterruptedException e) {
            // Uncheck it so we can throw it into Kafka
            // This exception could be thrown in practice
            throw new InterruptException(e);
        }

        if (local) {
            metrics.recordDuration(DurationType.WORKER_NEW_ORDER_LOCAL, timer);
        } else {
            metrics.recordDuration(DurationType.WORKER_NEW_ORDER_REMOTE, timer);
        }
    }



    private static Logger log = LogManager.getLogger();
}
