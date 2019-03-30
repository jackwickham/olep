package net.jackw.olep.utils.populate;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.DistrictSpecificKeySerde;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.Order;
import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.WarehouseSpecificKeySerde;
import net.jackw.olep.message.modification.ModificationKey;
import net.jackw.olep.message.modification.ModificationMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("FutureReturnValueIgnored")
public class PopulatableEventDatabase implements PopulatableDatabase {
    private final DatabaseConfig config;
    private final boolean populateImmutableStores;
    private final boolean populateMutableStores;

    private final Producer<Integer, Item> itemProducer;
    private final Producer<Integer, WarehouseShared> warehouseProducer;
    private final Producer<WarehouseSpecificKey, DistrictShared> districtProducer;
    private final Producer<DistrictSpecificKey, CustomerShared> customerProducer;
    private final Producer<WarehouseSpecificKey, StockShared> stockProducer;

    private final Producer<WarehouseSpecificKey, Integer> nextOrderIdStoreProducer;
    private final Producer<DistrictSpecificKey, CustomerMutable> customerMutableStoreProducer;
    private final Producer<WarehouseSpecificKey, List<NewOrder>> newOrderStoreProducer;
    private final Producer<WarehouseSpecificKey, Integer> stockQuantityStoreProducer;
    private final Producer<ModificationKey, ModificationMessage> modificationLogProducer;
    private final int storePartitions;

    private int transactionId = 0;

    private List<NewOrder> newOrdersFromThisDistrict;

    public PopulatableEventDatabase(DatabaseConfig config, boolean populateImmutableStores, boolean populateMutableStores) {
        this.config = config;
        this.populateImmutableStores = populateImmutableStores;
        this.populateMutableStores = populateMutableStores;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

        itemProducer = new KafkaProducer<>(props, Serdes.Integer().serializer(), new JsonSerializer<>());
        warehouseProducer = new KafkaProducer<>(props, Serdes.Integer().serializer(), new JsonSerializer<>());
        districtProducer = new KafkaProducer<>(props, WarehouseSpecificKeySerde.getInstance(), new JsonSerializer<>());
        customerProducer = new KafkaProducer<>(props, DistrictSpecificKeySerde.getInstance(), new JsonSerializer<>());
        stockProducer = new KafkaProducer<>(props, WarehouseSpecificKeySerde.getInstance(), new JsonSerializer<>());

        nextOrderIdStoreProducer = new KafkaProducer<>(props, WarehouseSpecificKeySerde.getInstance(), Serdes.Integer().serializer());
        customerMutableStoreProducer = new KafkaProducer<>(props, DistrictSpecificKeySerde.getInstance(), new JsonSerializer<>());
        newOrderStoreProducer = new KafkaProducer<>(props, WarehouseSpecificKeySerde.getInstance(), new JsonSerializer<>());
        stockQuantityStoreProducer = new KafkaProducer<>(props, WarehouseSpecificKeySerde.getInstance(), Serdes.Integer().serializer());

        storePartitions = nextOrderIdStoreProducer.partitionsFor(KafkaConfig.DISTRICT_NEXT_ORDER_ID_CHANGELOG).size();

        modificationLogProducer = new KafkaProducer<>(props, new ModificationKey.KeySerializer(), new JsonSerializer<>());
    }

    @Override
    public void populateItem(Item item) {
        itemProducer.send(new ProducerRecord<>(KafkaConfig.ITEM_IMMUTABLE_TOPIC, 0, item.id, item));
    }

    @Override
    public void populateWarehouse(WarehouseShared warehouse) {
        warehouseProducer.send(new ProducerRecord<>(KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC, 0, warehouse.id, warehouse));
    }

    @Override
    public void populateDistrict(DistrictShared district) {
        districtProducer.send(new ProducerRecord<>(KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, 0, district.getKey(), district));
        newOrdersFromThisDistrict = new ArrayList<>(config.getCustomersPerDistrict() / 3);
    }

    @Override
    public void populateCustomer(Customer customer) {
        if (populateImmutableStores) {
            customerProducer.send(new ProducerRecord<>(
                KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC, 0, customer.getKey(), customer.customerShared
            ));
        }
        if (populateMutableStores) {
            customerMutableStoreProducer.send(new ProducerRecord<>(
                KafkaConfig.CUSTOMER_MUTABLE_CHANGELOG, storePartition(customer.customerShared.warehouseId),
                customer.getKey(), customer.customerMutable
            ));
        }
    }

    @Override
    public void populateDeliveredOrder(OrderFactory.DeliveredOrder order) {
        int modificationPartition = order.newOrderModification.warehouseId % config.getModificationTopicPartitions();
        modificationLogProducer.send(new ProducerRecord<>(
            KafkaConfig.MODIFICATION_LOG, modificationPartition,
            new ModificationKey(++transactionId, (short) 0), order.newOrderModification
        ));
        modificationLogProducer.send(new ProducerRecord<>(
            KafkaConfig.MODIFICATION_LOG, modificationPartition,
            new ModificationKey(++transactionId, (short) 0), order.deliveryModification
        ));
    }

    @Override
    public void populateUndeliveredOrder(OrderFactory.UndeliveredOrder order) {
        int modificationPartition = order.newOrderModification.warehouseId % config.getModificationTopicPartitions();
        modificationLogProducer.send(new ProducerRecord<>(
            KafkaConfig.MODIFICATION_LOG, modificationPartition,
            new ModificationKey(++transactionId, (short) 0), order.newOrderModification
        ));
        newOrdersFromThisDistrict.add(order.newOrder);
    }

    @Override
    public void populateStock(Stock stock) {
        if (populateImmutableStores) {
            stockProducer.send(new ProducerRecord<>(
                KafkaConfig.STOCK_IMMUTABLE_TOPIC, 0, stock.getKey(), stock.stockShared)
            );
        }
        if (populateMutableStores) {
            stockQuantityStoreProducer.send(new ProducerRecord<>(
                KafkaConfig.STOCK_QUANTITY_CHANGELOG, storePartition(stock.stockShared.warehouseId), stock.getKey(), stock.stockQuantity
            ));
        }
    }

    @Override
    public void finaliseDistrict(DistrictShared district, int nextOrderId) {
        newOrderStoreProducer.send(new ProducerRecord<>(KafkaConfig.NEW_ORDER_CHANGELOG, storePartition(district.warehouseId), district.getKey(), newOrdersFromThisDistrict));
        nextOrderIdStoreProducer.send(new ProducerRecord<>(
            KafkaConfig.DISTRICT_NEXT_ORDER_ID_CHANGELOG, storePartition(district.warehouseId), district.getKey(), nextOrderId
        ));
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

    private int storePartition(int warehouseId) {
        return warehouseId % storePartitions;
    }
}
