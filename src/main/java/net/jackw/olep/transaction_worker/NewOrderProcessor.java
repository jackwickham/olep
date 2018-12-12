package net.jackw.olep.transaction_worker;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.Order;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.OrderLineResult;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import net.jackw.olep.utils.RandomDataGenerator;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;

/**
 * Process a New-Order transaction that affects this worker
 */
public class NewOrderProcessor extends BaseTransactionProcessor implements Processor<Long, NewOrderRequest> {
    private ProcessorContext context;
    private LocalStore<WarehouseSpecificKey, Integer> nextOrderIdStore;
    private LocalStore<WarehouseSpecificKey, Integer> stockQuantityStore;
    private NewOrdersStore newOrdersStore;

    private final SharedKeyValueStore<Integer, Item> itemStore;
    private final SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore;
    private final SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore;
    private final SharedKeyValueStore<DistrictSpecificKey, CustomerShared> customerImmutableStore;
    private final SharedKeyValueStore<WarehouseSpecificKey, StockShared> stockImmutableStore;

    private final RandomDataGenerator rand = new RandomDataGenerator();

    public NewOrderProcessor(
        SharedKeyValueStore<Integer, Item> itemStore,
        SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore,
        SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore,
        SharedKeyValueStore<DistrictSpecificKey, CustomerShared> customerImmutableStore,
        SharedKeyValueStore<WarehouseSpecificKey, StockShared> stockImmutableStore,
        int acceptedTransactionsPartitions
    ) {
        super(acceptedTransactionsPartitions);
        this.itemStore = itemStore;
        this.warehouseImmutableStore = warehouseImmutableStore;
        this.districtImmutableStore = districtImmutableStore;
        this.customerImmutableStore = customerImmutableStore;
        this.stockImmutableStore = stockImmutableStore;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;

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
    public void process(Long key, NewOrderRequest value) {
        System.out.printf("Processing transaction %d\n", key);
        try {
            final NewOrderResult.PartialResult results = new NewOrderResult.PartialResult();

            boolean remote = true;
            if (isProcessorForWarehouse(value.warehouseId, context)) {
                // This worker is responsible for the home warehouse
                remote = false;

                WarehouseSpecificKey districtKey = new WarehouseSpecificKey(value.districtId, value.warehouseId);

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
                results.customerSurname = customer.lastName;
                results.credit = customer.credit;
                results.discount = customer.discount;
                results.warehouseTax = warehouse.tax;
                results.districtTax = district.tax;

                // Create the order builder, so we can put the line items into it
                OrderBuilder orderBuilder = new OrderBuilder(
                    orderId, value.districtId, value.warehouseId, value.customerId, value.date
                );

                int nextLineNumber = 0;
                for (NewOrderRequest.OrderLine line : value.lines) {
                    int lineNumber = nextLineNumber++;
                    WarehouseSpecificKey stockKey = new WarehouseSpecificKey(line.itemId, line.supplyingWarehouseId);

                    // Load the item and stock data for this line
                    Item item = itemStore.getBlocking(line.itemId);
                    StockShared stockShared = stockImmutableStore.getBlocking(stockKey);

                    // Add the order line to the generated order
                    BigDecimal lineAmount = item.price.multiply(new BigDecimal(line.quantity));
                    OrderLine orderLine = new OrderLine(
                        orderId, value.districtId, value.warehouseId, lineNumber, line.itemId,
                        line.supplyingWarehouseId, line.quantity, lineAmount,
                        stockShared.getDistrictInfo(value.districtId)
                    );

                    orderBuilder.addOrderLine(orderLine);

                    results.addLine(lineNumber, new OrderLineResult.PartialResult(
                        item.name, item.price, lineAmount
                    ));
                }

                newOrdersStore.add(districtKey, orderBuilder.buildNewOrder());

                //Order order = orderBuilder.build();
                // We don't actually need to build the order at all
                // TODO: If it isn't needed anywhere, we can remove it entirely

                // Forward the transaction to the modification log
                // We might want to add extra data to the NewOrderModification here in future, depending on what is
                // actually used by the views, but this corresponds with the event sourcing model
                context.forward(key, new NewOrderModification(value, orderId), To.child("modification-log"));
            }

            int nextLineNumber = 0;
            for (NewOrderRequest.OrderLine line : value.lines) {
                int lineNumber = nextLineNumber++;

                if (!isProcessorForWarehouse(line.supplyingWarehouseId, context)) {
                    // Not responsible for this warehouse
                    continue;
                }

                Item item = itemStore.getBlocking(line.itemId);

                WarehouseSpecificKey stockKey = new WarehouseSpecificKey(item.id, line.supplyingWarehouseId);

                int stockQuantity = stockQuantityStore.get(stockKey);
                int excessStock = stockQuantity - line.quantity;
                if (excessStock < 10) {
                    stockQuantity += 91;
                }
                stockQuantity -= line.quantity;
                stockQuantityStore.put(stockKey, stockQuantity);

                // The other changes are performed in the view if needed, based on the event that is sent by the worker
                // for the owning warehouse

                results.addLine(lineNumber, new OrderLineResult.PartialResult(stockQuantity));
            }

            TransactionResultMessage resultMessage = new TransactionResultMessage(key, results);
            context.forward(key, resultMessage, To.child("transaction-results"));
        } catch (InterruptedException e) {
            // Uncheck it so we can throw it into Kafka
            // This exception could be thrown in practice
            throw new InterruptException(e);
        }
    }

    @Override
    public void close() {

    }
}
