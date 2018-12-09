package net.jackw.olep.transaction_worker;

import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.Order;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.message.transaction_request.NewOrderMessage;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Process a New-Order transaction that affects this worker
 */
// TODO: Is this the right key?
public class NewOrderProcessor implements Processor<Long, NewOrderMessage> {
    private ProcessorContext context;

    private final SharedKeyValueStore<Integer, Item> itemStore;
    private final SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore;
    private final SharedKeyValueStore<DistrictShared.Key, DistrictShared> districtImmutableStore;
    private final SharedKeyValueStore<CustomerShared.Key, CustomerShared> customerImmutableStore;
    private final SharedKeyValueStore<StockShared.Key, StockShared> stockImmutableStore;

    public NewOrderProcessor(
        SharedKeyValueStore<Integer, Item> itemStore,
        SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore,
        SharedKeyValueStore<DistrictShared.Key, DistrictShared> districtImmutableStore,
        SharedKeyValueStore<CustomerShared.Key, CustomerShared> customerImmutableStore,
        SharedKeyValueStore<StockShared.Key, StockShared> stockImmutableStore
    ) {
       this.itemStore = itemStore;
       this.warehouseImmutableStore = warehouseImmutableStore;
       this.districtImmutableStore = districtImmutableStore;
       this.customerImmutableStore = customerImmutableStore;
       this.stockImmutableStore = stockImmutableStore;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(Long key, NewOrderMessage value) {
        NewOrderResult.PartialResult results = new NewOrderResult.PartialResult();

        boolean remote = true;
        if (getWarehouses().contains(value.warehouseId)) {
            // This worker is responsible for the home warehouse
            remote = false;

            // Load values from the immutable stores
            WarehouseShared warehouse;
            DistrictShared district;
            CustomerShared customer;

            // It's possible that the stores aren't fully loaded yet, so retry with backoff if we fail to load values
            int attempts = 0;
            do {
                warehouse = warehouseImmutableStore.get(value.warehouseId);
                district = districtImmutableStore.get(new DistrictShared.Key(value.districtId, value.warehouseId));
                customer = customerImmutableStore.get(
                    new CustomerShared.Key(value.customerId, value.districtId, value.warehouseId)
                );
                if (warehouse != null && district != null && customer != null) {
                    break;
                }
                // Not loaded yet
                if (attempts > 10) {
                    // If this happens, we have a problem
                    throw new RuntimeException(String.format("Failed to load relevant records from immutable stores " +
                        " - warehouse %d loaded=%s, district %d loaded=%s, customer %d loaded=%s", value.warehouseId,
                        warehouse == null, value.districtId, district == null, value.customerId, customer == null));
                }
                // Wait a bit, then try again
                try {
                    Thread.sleep(100 + 100*attempts++);
                } catch (InterruptedException e) {
                    // Uncheck it so we can throw it into Kafka
                    // This exception could be thrown in practice
                    throw new InterruptException(e);
                }
            } while (true);

            // TODO: Load next order ID from worker's key-value store
            int orderId = -1;

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

            int lineNumber = 0;
            for (NewOrderMessage.OrderLine line : value.lines) {
                Item item = itemStore.get(line.itemId); // TODO: failure checks
                StockShared stockShared = stockImmutableStore.get(
                    new StockShared.Key(line.itemId, line.supplyingWarehouseId)
                );

                BigDecimal lineAmount = item.price.multiply(new BigDecimal(line.quantity));
                OrderLine orderLine = new OrderLine(
                    orderId, value.districtId, value.warehouseId, ++lineNumber, line.itemId, line.supplyingWarehouseId,
                    line.quantity, lineAmount, stockShared.getDistrictInfo(value.districtId)
                );

                orderBuilder.addOrderLine(orderLine);

                results.lines.add(new NewOrderResult.OrderLineResult(
                    // TODO: stock quantity has to be done by the dispatching warehouse
                    line.supplyingWarehouseId, line.itemId, item.name, line.quantity, -1, item.price,
                    lineAmount
                ));
            }

            // TODO: Add New-Order (= Order.Key) to the new order queue

            Order order = orderBuilder.build();
            // TODO: Add this order to the modification log (transactionally)
        }

        value.lines.stream().filter(line -> getWarehouses().contains(line.supplyingWarehouseId)).forEach(line -> {
            // This worker is responsible for the warehouse where this item is dispatched from

            Item item;
            do {
                item = itemStore.get(line.itemId);
            } while (item == null); // TODO: backoff

            // TODO: Load stock quantity from worker kv store
            // TODO: Update stock ytd, order_cnt, quantity and possibly remoteCnt

            // TODO: figure out stock quantity
        });

        // TODO: Send one transaction to the modification log with all the changes

        TransactionResultMessage resultMessage = new TransactionResultMessage(key, results);
        context.forward(key, resultMessage, To.child("transaction-results"));
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
