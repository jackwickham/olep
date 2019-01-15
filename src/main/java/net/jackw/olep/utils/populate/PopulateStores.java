package net.jackw.olep.utils.populate;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.message.modification.ModificationMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("FutureReturnValueIgnored")
public class PopulateStores implements AutoCloseable {
    private String bootstrapServers = "localhost:9092";
    private Properties props;

    private Producer<Integer, Item> itemProducer;
    private Producer<Integer, WarehouseShared> warehouseProducer;
    private Producer<WarehouseSpecificKey, DistrictShared> districtProducer;
    private Producer<DistrictSpecificKey, CustomerShared> customerProducer;
    private Producer<WarehouseSpecificKey, StockShared> stockProducer;

    private Producer<WarehouseSpecificKey, Integer> nextOrderIdStoreProducer;
    private Producer<DistrictSpecificKey, CustomerMutable> customerMutableStoreProducer;
    private Producer<WarehouseSpecificKey, List<NewOrder>> newOrderStoreProducer;
    private Producer<WarehouseSpecificKey, Integer> stockQuantityStoreProducer;
    private int storePartitions;

    private Producer<Long, ModificationMessage> modificationLogProducer;

    private int itemCount;
    private int warehouseCount;
    private int districtsPerWarehouse;
    private int customersPerDistrict;
    private int customerNameRange;
    private boolean predictable;

    private ArrayList<Integer> customerIds;

    private long transactionId = 0;

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public PopulateStores(int itemCount, int warehouseCount, int districtsPerWarehouse, int customersPerDistrict, int customerNameRange, boolean predictable) {
        this.itemCount = itemCount;
        this.warehouseCount = warehouseCount;
        this.districtsPerWarehouse = districtsPerWarehouse;
        this.customersPerDistrict = customersPerDistrict;
        this.customerNameRange = customerNameRange;
        this.predictable = predictable;

        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        itemProducer = new KafkaProducer<>(props, Serdes.Integer().serializer(), new JsonSerializer<>());
        warehouseProducer = new KafkaProducer<>(props, Serdes.Integer().serializer(), new JsonSerializer<>());
        districtProducer = new KafkaProducer<>(props, new JsonSerializer<>(), new JsonSerializer<>());
        customerProducer = new KafkaProducer<>(props, new JsonSerializer<>(), new JsonSerializer<>());
        stockProducer = new KafkaProducer<>(props, new JsonSerializer<>(), new JsonSerializer<>());

        nextOrderIdStoreProducer = new KafkaProducer<>(props, new JsonSerializer<>(), Serdes.Integer().serializer());
        customerMutableStoreProducer = new KafkaProducer<>(props, new JsonSerializer<>(), new JsonSerializer<>());
        newOrderStoreProducer = new KafkaProducer<>(props, new JsonSerializer<>(), new JsonSerializer<>());
        stockQuantityStoreProducer = new KafkaProducer<>(props, new JsonSerializer<>(), Serdes.Integer().serializer());

        storePartitions = nextOrderIdStoreProducer.partitionsFor(changelog(KafkaConfig.DISTRICT_NEXT_ORDER_ID_STORE)).size();

        modificationLogProducer = new KafkaProducer<>(props, Serdes.Long().serializer(), new JsonSerializer<>());

        customerIds = IntStream.range(1, customersPerDistrict+1).boxed().collect(Collectors.toCollection(ArrayList::new));
    }

    public static void main(String[] args) {
        try (PopulateStores i = new PopulateStores(20, 100, 10, 100, 20, false)) {
            i.populate();
        }
    }

    public void populate() {
        populateItems();
        populateWarehouses();
    }

    private void populateItems() {
        ItemFactory factory;
        if (predictable) {
            factory = PredictableItemFactory.getInstance();
        } else {
            factory = RandomItemFactory.getInstance();
        }

        for (int i = 0; i < itemCount; i++) {
            Item item = factory.makeItem();
            itemProducer.send(new ProducerRecord<>(KafkaConfig.ITEM_IMMUTABLE_TOPIC, 0, item.id, item));
        }
        itemProducer.flush();
    }

    private void populateWarehouses() {
        WarehouseFactory factory;
        if (predictable) {
            factory = PredictableWarehouseFactory.getInstance();
        } else {
            factory = RandomWarehouseFactory.getInstance();
        }

        for (int wh = 0; wh < warehouseCount; wh++) {
            WarehouseShared warehouse = factory.makeWarehouseShared();
            warehouseProducer.send(new ProducerRecord<>(KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC, 0, warehouse.id, warehouse));

            int[] stockQuantities = populateStock(warehouse);
            populateDistricts(warehouse, stockQuantities);
        }
        warehouseProducer.flush();
    }

    private void populateDistricts(WarehouseShared warehouse, int[] stockQuantities) {
        DistrictFactory districtFactory;
        if (predictable) {
            districtFactory = PredictableDistrictFactory.instanceFor(warehouse);
        } else {
            districtFactory = RandomDistrictFactory.instanceFor(warehouse);
        }
        for (int dst = 0; dst < districtsPerWarehouse; dst++) {
            DistrictShared district = districtFactory.makeDistrictShared();
            districtProducer.send(new ProducerRecord<>(KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, 0, district.getKey(), district));

            populateCustomers(district);
            populateOrders(district, stockQuantities);
        }
    }

    private void populateCustomers(DistrictShared district) {
        CustomerFactory customerFactory;
        if (predictable) {
            customerFactory = PredictableCustomerFactory.instanceFor(district, customerNameRange);
        } else {
            customerFactory = RandomCustomerFactory.instanceFor(district, customerNameRange);
        }
        for (int cust = 0; cust < customersPerDistrict; cust++) {
            Customer customer = customerFactory.makeCustomer();
            customerProducer.send(new ProducerRecord<>(
                KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC, 0, customer.getKey(), customer.customerShared
            ));
            customerMutableStoreProducer.send(new ProducerRecord<>(
                changelog(KafkaConfig.CUSTOMER_MUTABLE_STORE), storePartition(district.warehouseId), customer.getKey(), customer.customerMutable
            ));
        }
    }

    private int[] populateStock(WarehouseShared warehouse) {
        StockFactory stockFactory;
        if (predictable) {
            stockFactory = PredictableStockFactory.instanceFor(warehouse);
        } else {
            stockFactory = RandomStockFactory.instanceFor(warehouse);
        }
        int[] stockQuantities = new int[itemCount];
        for (int item = 0; item < itemCount; item++) {
            Stock stock = stockFactory.makeStock();
            stockProducer.send(new ProducerRecord<>(
                KafkaConfig.STOCK_IMMUTABLE_TOPIC, 0, stock.getKey(), stock.stockShared)
            );
            stockQuantityStoreProducer.send(new ProducerRecord<>(
                changelog(KafkaConfig.STOCK_QUANTITY_STORE), storePartition(warehouse.id), stock.getKey(), stock.stockQuantity
            ));
            stockQuantities[item] = stock.stockQuantity;
        }
        return stockQuantities;
    }

    private void populateOrders(DistrictShared district, int[] stockQuantities) {
        OrderFactory orderFactory;
        if (predictable) {
            orderFactory = PredictableOrderFactory.instanceFor(district, itemCount);
        } else {
            orderFactory = RandomOrderFactory.instanceFor(district, itemCount);
            Collections.shuffle(customerIds);
        }
        OrderFactory.StockProvider stockProvider = itemId -> stockQuantities[itemId-1];
        int processedCustomers = 0;
        List<NewOrder> newOrders = new ArrayList<>(customersPerDistrict / 3);
        for (int customerId : customerIds) {
            if (processedCustomers++ < customersPerDistrict * 2 / 3) {
                // For 2/3 of the customers, the order has been delivered already
                OrderFactory.DeliveredOrder order = orderFactory.makeDeliveredOrder(customerId, stockProvider);
                modificationLogProducer.send(new ProducerRecord<>(
                    KafkaConfig.MODIFICATION_LOG, 0, ++transactionId, order.newOrderModification
                ));
                modificationLogProducer.send(new ProducerRecord<>(
                    KafkaConfig.MODIFICATION_LOG, 0, ++transactionId, order.deliveryModification
                ));
            } else {
                // For the remaining 1/3, the order is still outstanding, so we add it as a NewOrder too
                OrderFactory.UndeliveredOrder order = orderFactory.makeUndeliveredOrder(customerId, stockProvider);
                modificationLogProducer.send(new ProducerRecord<>(
                    KafkaConfig.MODIFICATION_LOG, 0, ++transactionId, order.newOrderModification
                ));
                newOrders.add(order.newOrder);
            }
        }
        newOrderStoreProducer.send(new ProducerRecord<>(changelog(KafkaConfig.NEW_ORDER_STORE), storePartition(district.warehouseId), district.getKey(), newOrders));
    }

    @Override
    public void close() {
        warehouseProducer.close();
        districtProducer.close();
        customerProducer.close();
        stockProducer.close();

        nextOrderIdStoreProducer.close();
        customerMutableStoreProducer.close();
        newOrderStoreProducer.close();
        stockQuantityStoreProducer.close();
    }

    private String changelog(String storeName) {
        return "worker-" + storeName + "-changelog";
    }

    private int storePartition(int warehouseId) {
        return warehouseId % storePartitions;
    }
}
