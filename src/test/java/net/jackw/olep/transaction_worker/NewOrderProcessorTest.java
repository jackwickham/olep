package net.jackw.olep.transaction_worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.SharedCustomerStore;
import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.OrderLineResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class NewOrderProcessorTest {
    private NewOrderProcessor processor;
    private MockProcessorContext context;

    private KeyValueStore<WarehouseSpecificKey, Integer> nextOrderIdStore;
    private KeyValueStore<WarehouseSpecificKey, Integer> stockQuantityStore;
    private KeyValueStore<WarehouseSpecificKey, ArrayDeque<NewOrder>> newOrdersStore;

    @Mock(lenient = true)
    private SharedKeyValueStore<Integer, Item> itemStore;
    @Mock(lenient = true)
    private SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore;
    @Mock(lenient = true)
    private SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore;
    @Mock(lenient = true)
    private SharedCustomerStore customerImmutableStore;
    @Mock(lenient = true)
    private SharedKeyValueStore<WarehouseSpecificKey, StockShared> stockImmutableStore;

    private Item[] items = new Item[3];
    private WarehouseShared warehouseShared;
    private DistrictShared districtShared;
    private CustomerShared customerShared;
    private StockShared[] stock = new StockShared[6];

    @Before
    public void setUp() throws InterruptedException {
        processor = new NewOrderProcessor(
            itemStore, warehouseImmutableStore, districtImmutableStore, customerImmutableStore, stockImmutableStore, 3
        );
        context = new MockProcessorContext();

        nextOrderIdStore = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(KafkaConfig.DISTRICT_NEXT_ORDER_ID_STORE),
            new JsonSerde<>(WarehouseSpecificKey.class),
            Serdes.Integer()
        ).withLoggingDisabled().build();
        nextOrderIdStore.init(context, nextOrderIdStore);
        context.register(nextOrderIdStore, null);

        stockQuantityStore = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(KafkaConfig.STOCK_QUANTITY_STORE),
            new JsonSerde<>(WarehouseSpecificKey.class),
            Serdes.Integer()
        ).withLoggingDisabled().build();
        stockQuantityStore.init(context, stockQuantityStore);
        context.register(stockQuantityStore, null);

        newOrdersStore = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(KafkaConfig.NEW_ORDER_STORE),
            new JsonSerde<>(WarehouseSpecificKey.class),
            new JsonSerde<ArrayDeque<NewOrder>>(new TypeReference<>() {})
        ).withLoggingDisabled().build();
        newOrdersStore.init(context, newOrdersStore);
        context.register(newOrdersStore, null);

        processor.init(context);


        //// Populate immutable stores ////
        // Items
        items[0] = new Item(0, 156, "ITEM 0", new BigDecimal("10"), "d0");
        when(itemStore.getBlocking(0)).thenReturn(items[0]);
        items[1] = new Item(1, 157, "ITEM 1", new BigDecimal("11.11"), "d1");
        when(itemStore.getBlocking(1)).thenReturn(items[1]);
        items[2] = new Item(2, 158, "ITEM 2", new BigDecimal("12.22"), "d2");
        when(itemStore.getBlocking(2)).thenReturn(items[2]);

        // Warehouses
        warehouseShared = new WarehouseShared(
            3, "Warehouse 3", new Address("warehouse", "", "", "", ""), new BigDecimal("0.12")
        );
        when(warehouseImmutableStore.getBlocking(3)).thenReturn(warehouseShared);

        // Districts
        districtShared = new DistrictShared(
            4, 3, "District 4", new Address("dist", "", "", "", ""), new BigDecimal("0.05")
        );
        when(districtImmutableStore.getBlocking(new WarehouseSpecificKey(4, 3))).thenReturn(districtShared);

        // Customer
        customerShared = new CustomerShared(
            5, 4, 3, "FN", "MN", "LN", new Address("cust", "", "", "", ""),
            "0123456789", 167L, Credit.BC, new BigDecimal("123.45"), new BigDecimal("0.079")
        );
        when(customerImmutableStore.getBlocking(new DistrictSpecificKey(5, 4, 3))).thenReturn(customerShared);

        // Stock
        stock[0] = new StockShared(0, 3, "", "", "", "dst0", "", "", "", "", "", "", "stock0");
        when(stockImmutableStore.getBlocking(new WarehouseSpecificKey(0, 3))).thenReturn(stock[0]);
        stock[1] = new StockShared(1, 3, "", "", "", "dst1", "", "", "", "", "", "", "stock1");
        when(stockImmutableStore.getBlocking(new WarehouseSpecificKey(1, 3))).thenReturn(stock[1]);
        stock[2] = new StockShared(2, 3, "", "", "", "dst2", "", "", "", "", "", "", "stock2");
        when(stockImmutableStore.getBlocking(new WarehouseSpecificKey(2, 3))).thenReturn(stock[2]);

        stock[3] = new StockShared(0, 4, "", "", "", "dst3", "", "", "", "", "", "", "stock0");
        when(stockImmutableStore.getBlocking(new WarehouseSpecificKey(0, 4))).thenReturn(stock[3]);
        stock[4] = new StockShared(1, 4, "", "", "", "dst4", "", "", "", "", "", "", "stock1");
        when(stockImmutableStore.getBlocking(new WarehouseSpecificKey(1, 4))).thenReturn(stock[4]);
        stock[5] = new StockShared(2, 4, "", "", "", "dst5", "", "", "", "", "", "", "stock2");
        when(stockImmutableStore.getBlocking(new WarehouseSpecificKey(2, 4))).thenReturn(stock[5]);

        //// Mutable stores ////
        // Next order ID is deliberately left empty

        // Stock
        stockQuantityStore.put(new WarehouseSpecificKey(0, 3), 50);
        stockQuantityStore.put(new WarehouseSpecificKey(1, 3), 11);
        // Not populating the store for item 2

        // New orders is deliberately left empty

        // Set partition for the message so warehouse 3 is associated with this processor
        context.setPartition(0);
    }

    @After
    public void tearDown() {
        nextOrderIdStore.close();
        stockQuantityStore.close();
        newOrdersStore.close();
    }

    @Test
    public void testHomeWarehouseAddsToModificationLog() {
        NewOrderRequest request = new NewOrderRequest(5, 4, 3, ImmutableList.of(
            new NewOrderRequest.OrderLine(0, 4, 3),
            new NewOrderRequest.OrderLine(1, 4, 5)
        ), 5L);
        processor.process(50L, request);

        List<MockProcessorContext.CapturedForward> forwards = context.forwarded();

        Map<String, KeyValue> forwardsTo = forwards.stream().collect(Collectors.toMap(f -> f.childName(), f -> f.keyValue()));
        assertThat(forwardsTo, Matchers.hasKey(KafkaConfig.MODIFICATION_LOG));
        assertThat(forwardsTo, Matchers.hasKey(KafkaConfig.TRANSACTION_RESULT_TOPIC));

        assertEquals(50L, forwardsTo.get(KafkaConfig.MODIFICATION_LOG).key);

        NewOrderModification modification = (NewOrderModification) forwardsTo.get(KafkaConfig.MODIFICATION_LOG).value;
        assertEquals(5, modification.customerId);
        assertEquals(3, modification.warehouseId);
        assertEquals(4, modification.districtId);
        assertEquals(5L, modification.date);
        assertEquals("default order ID should be 1", 1, modification.orderId);

        assertThat(modification.lines, Matchers.hasSize(2));
        OrderLine line0 = modification.lines.get(0);
        assertEquals(0, line0.lineNumber);
        assertEquals(0, line0.itemId);
        assertEquals(4, line0.supplyWarehouseId);
        assertNull(line0.deliveryDate);
        assertEquals(3, line0.quantity);
        assertEquals(new BigDecimal("30"), line0.amount);
        assertEquals(stock[3].dist04, line0.distInfo);
        OrderLine line1 = modification.lines.get(1);
        assertEquals(1, line1.lineNumber);
        assertEquals(1, line1.itemId);
        assertEquals(4, line1.supplyWarehouseId);
        assertNull(line1.deliveryDate);
        assertEquals(5, line1.quantity);
        assertEquals(new BigDecimal("55.55"), line1.amount);
        assertEquals(stock[4].dist04, line1.distInfo);
    }

    @Test
    public void testRemoteWarehouseDoesntAddToModificationLog() {
        NewOrderRequest request = new NewOrderRequest(5, 4, 4, ImmutableList.of(), 5L);
        processor.process(50L, request);

        assertThat(context.forwarded(KafkaConfig.MODIFICATION_LOG), Matchers.empty());
    }

    @Test
    public void testHomeWarehouseResultsContainAlmostAllDetails() {
        NewOrderRequest request = new NewOrderRequest(5, 4, 3, ImmutableList.of(
            new NewOrderRequest.OrderLine(0, 4, 3),
            new NewOrderRequest.OrderLine(1, 4, 5)
        ), 5L);
        processor.process(50L, request);

        NewOrderResult.PartialResult result = (NewOrderResult.PartialResult)
            context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC).get(0).keyValue().value;

        assertEquals(1, (int) result.orderId);
        assertEquals(customerShared.lastName, result.customerLastName);
        assertEquals(customerShared.credit, result.credit);
        assertEquals(customerShared.discount, result.discount);
        assertEquals(warehouseShared.tax, result.warehouseTax);
        assertEquals(districtShared.tax, result.districtTax);

        assertThat(result.getLines().values(), Matchers.hasSize(2));
        OrderLineResult.PartialResult line0 = result.getLines().get(0);
        assertEquals(items[0].name, line0.itemName);
        assertEquals(items[0].price, line0.itemPrice);
        assertEquals(new BigDecimal("30"), line0.lineAmount);
        assertNull(line0.stockQuantity);
        OrderLineResult.PartialResult line1 = result.getLines().get(1);
        assertEquals(items[1].name, line1.itemName);
        assertEquals(items[1].price, line1.itemPrice);
        assertEquals(new BigDecimal("55.55"), line1.lineAmount);
        assertNull(line1.stockQuantity);
    }

    @Test
    public void testRemoteWarehouseResultsContainsStockQuantityOnly() {
        NewOrderRequest request = new NewOrderRequest(5, 4, 4, ImmutableList.of(
            new NewOrderRequest.OrderLine(0, 3, 3),
            new NewOrderRequest.OrderLine(1, 3, 5)
        ), 5L);
        processor.process(50L, request);

        assertThat(context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC), Matchers.hasSize(1));

        NewOrderResult.PartialResult result = (NewOrderResult.PartialResult)
            context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC).get(0).keyValue().value;

        assertNull(result.orderId);
        assertNull(result.customerLastName);
        assertNull(result.credit);
        assertNull(result.discount);
        assertNull(result.warehouseTax);
        assertNull(result.districtTax);

        assertThat(result.getLines().values(), Matchers.hasSize(2));
        OrderLineResult.PartialResult line0 = result.getLines().get(0);
        assertNull(line0.itemName);
        assertNull(line0.itemPrice);
        assertNull(line0.lineAmount);
        assertEquals(47, (int) line0.stockQuantity);
        OrderLineResult.PartialResult line1 = result.getLines().get(1);
        assertNull(line1.itemName);
        assertNull(line1.itemPrice);
        assertNull(line1.lineAmount);
        assertEquals(11 - 5 + 91, (int) line1.stockQuantity);
    }

    @Test
    public void testResultsMergedWhenDispatchedFromHomeWarehouse() {
        NewOrderRequest request = new NewOrderRequest(5, 4, 3, ImmutableList.of(
            new NewOrderRequest.OrderLine(0, 3, 3),
            new NewOrderRequest.OrderLine(1, 4, 5)
        ), 5L);
        processor.process(50L, request);

        assertThat(context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC), Matchers.hasSize(1));

        NewOrderResult.PartialResult result = (NewOrderResult.PartialResult)
            context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC).get(0).keyValue().value;

        // Make sure the regular data was still populated
        assertEquals(1, (int) result.orderId);
        assertEquals(customerShared.lastName, result.customerLastName);

        assertThat(result.getLines().values(), Matchers.hasSize(2));
        // The first line should be fully populated
        OrderLineResult.PartialResult line0 = result.getLines().get(0);
        assertEquals(items[0].name, line0.itemName);
        assertEquals(items[0].price, line0.itemPrice);
        assertEquals(new BigDecimal("30"), line0.lineAmount);
        assertEquals(47, (int) line0.stockQuantity);
        // The second line should not
        OrderLineResult.PartialResult line1 = result.getLines().get(1);
        assertNotNull(line1.itemName);
        assertNotNull(line1.itemPrice);
        assertNotNull(line1.lineAmount);
        assertNull(line1.stockQuantity);
    }

    @Test
    public void orderIdIncrementedAndWrittenToStore() {
        WarehouseSpecificKey districtKey = new WarehouseSpecificKey(4, 3);
        NewOrderRequest request = new NewOrderRequest(5, 4, 3, ImmutableList.of(), 5L);
        processor.process(50L, request);

        NewOrderResult.PartialResult result = (NewOrderResult.PartialResult)
            context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC).get(0).keyValue().value;
        assertEquals(1, (int) result.orderId);
        assertEquals("First order id not persisted", 2, (int) nextOrderIdStore.get(districtKey));

        context.resetForwards();
        processor.process(51L, request);

        NewOrderResult.PartialResult result2 = (NewOrderResult.PartialResult)
            context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC).get(0).keyValue().value;
        assertEquals(2, (int) result2.orderId);
        assertEquals(3, (int) nextOrderIdStore.get(districtKey));
    }

    @Test
    public void testOrderInsertedIntoNewOrdersStore() {
        WarehouseSpecificKey districtKey = new WarehouseSpecificKey(4, 3);
        NewOrderRequest request = new NewOrderRequest(5, 4, 3, ImmutableList.of(
            new NewOrderRequest.OrderLine(0, 4, 3),
            new NewOrderRequest.OrderLine(1, 4, 5)
        ), 5L);

        processor.process(50L, request);
        processor.process(51L, request);

        ArrayDeque<NewOrder> newOrders = newOrdersStore.get(districtKey);
        assertThat(newOrders, Matchers.hasSize(2));
        NewOrder order1 = newOrders.pop();
        assertEquals(1, order1.orderId);
        assertEquals(3, order1.warehouseId);
        assertEquals(4, order1.districtId);
        assertEquals(5, order1.customerId);
        assertEquals(new BigDecimal("85.55"), order1.totalAmount);

        NewOrder order2 = newOrders.pop();
        assertEquals(2, order2.orderId);
        assertEquals(3, order2.warehouseId);
        assertEquals(4, order2.districtId);
        assertEquals(5, order2.customerId);
        assertEquals(new BigDecimal("85.55"), order2.totalAmount);
    }

    @Test
    public void testStockQuantityGeneratedCorrectly() {
        NewOrderRequest request = new NewOrderRequest(5, 4, 4, ImmutableList.of(
            new NewOrderRequest.OrderLine(2, 3, 3)
        ), 5L);
        processor.process(50L, request);

        assertThat(context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC), Matchers.hasSize(1));

        NewOrderResult.PartialResult result = (NewOrderResult.PartialResult)
            context.forwarded(KafkaConfig.TRANSACTION_RESULT_TOPIC).get(0).keyValue().value;

        assertThat(result.getLines().values(), Matchers.hasSize(1));
        OrderLineResult.PartialResult line0 = result.getLines().get(0);
        assertThat(
            line0.stockQuantity,
            Matchers.both(Matchers.greaterThanOrEqualTo(10)).and(Matchers.lessThanOrEqualTo(100))
        );

        assertEquals(line0.stockQuantity, stockQuantityStore.get(new WarehouseSpecificKey(2, 3)));
    }
}
