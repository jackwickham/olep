package net.jackw.olep.transaction_worker;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.Order;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
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
import java.util.HashSet;
import java.util.Set;

/**
 * Process a New-Order transaction that affects this worker
 */
// TODO: Is this the right key?
public class NewOrderProcessor implements Processor<Long, NewOrderRequest> {
    private ProcessorContext context;
    private LocalStore<WarehouseSpecificKey, Integer> nextOrderIdStore;
    private LocalStore<WarehouseSpecificKey, Integer> stockQuantityStore;

    private final SharedKeyValueStore<Integer, Item> itemStore;
    private final SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore;
    private final SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore;
    private final SharedKeyValueStore<DistrictSpecificKey, CustomerShared> customerImmutableStore;
    private final SharedKeyValueStore<WarehouseSpecificKey, StockShared> stockImmutableStore;

    public NewOrderProcessor(
        SharedKeyValueStore<Integer, Item> itemStore,
        SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore,
        SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore,
        SharedKeyValueStore<DistrictSpecificKey, CustomerShared> customerImmutableStore,
        SharedKeyValueStore<WarehouseSpecificKey, StockShared> stockImmutableStore
    ) {
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
        final RandomDataGenerator rand = new RandomDataGenerator();

        this.nextOrderIdStore = new LocalStore(
            (KeyValueStore) context.getStateStore(KafkaConfig.DISTRICT_NEXT_ORDER_ID_STORE), 1
        );
        this.stockQuantityStore = new LocalStore<WarehouseSpecificKey, Integer>(
            (KeyValueStore) context.getStateStore(KafkaConfig.STOCK_QUANTITY_STORE),
            // Default value is S_QUANTITY random within [10 .. 100]
            () -> rand.uniform(10, 100)
        );
    }

    @Override
    public void process(Long key, NewOrderRequest value) {
        try {
            final NewOrderResult.PartialResult results = new NewOrderResult.PartialResult();

            boolean remote = true;
            if (getWarehouses().contains(value.warehouseId)) {
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

                // TODO: Add New-Order (= Order.Key) to the new order queue

                Order order = orderBuilder.build();
                // TODO: Add this order to the modification log (transactionally)
            }

            int nextLineNumber = 0;
            for (NewOrderRequest.OrderLine line : value.lines) {
                int lineNumber = nextLineNumber++;

                if (!getWarehouses().contains(line.supplyingWarehouseId)) {
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

                // TODO: Update stock ytd, order_cnt, quantity and possibly remoteCnt in the modification log

                results.addLine(lineNumber, new OrderLineResult.PartialResult(stockQuantity));
            }

            // TODO: Send one transaction to the modification log with all the changes

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

    private Set<Integer> getWarehouses() {
        // TODO
        Set<Integer> warehouses = new HashSet<>(100);
        for (int i = 1; i <= 100; i++) {
            warehouses.add(i);
        }
        return warehouses;
    }
}
