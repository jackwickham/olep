package net.jackw.olep.utils.immutable_stores;

import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

@SuppressWarnings("FutureReturnValueIgnored")
public class PopulateStores {
    private String bootstrapServers = "localhost:9092";
    private Properties props;

    private int itemCount = 100;
    private int warehouseCount = 100;
    private int districtsPerWarehouse = 10;
    private int customersPerDistrict = 1000;

    public PopulateStores() {
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public static void main(String[] args) {
        PopulateStores i = new PopulateStores();
        i.populateItems();
        i.populateWarehouses();
    }

    private void populateItems() {
        ItemFactory factory = ItemFactory.getInstance();

        // Set up the producer, which is used to send requests from the application to the DB
        Serializer<Item> serializer = new JsonSerializer<>(Item.class);

        try (Producer<Integer, Item> producer = new KafkaProducer<>(props, Serdes.Integer().serializer(), serializer)) {
            for (int i = 0; i < itemCount; i++) {
                Item item = factory.makeItem();
                producer.send(new ProducerRecord<>(KafkaConfig.ITEM_IMMUTABLE_TOPIC, 0, item.id, item));
            }
            producer.flush();
        }
    }

    private void populateWarehouses() {
        WarehouseFactory factory = WarehouseFactory.getInstance();

        try (
            Producer<Integer, WarehouseShared> warehouseProducer = new KafkaProducer<>(props, Serdes.Integer().serializer(), new JsonSerializer<>(WarehouseShared.class));
            Producer<DistrictShared.Key, DistrictShared> districtProducer = new KafkaProducer<>(props, new JsonSerializer<>(DistrictShared.Key.class), new JsonSerializer<>(DistrictShared.class));
            Producer<CustomerShared.Key, CustomerShared> customerProducer = new KafkaProducer<>(props, new JsonSerializer<>(CustomerShared.Key.class), new JsonSerializer<>(CustomerShared.class));
            Producer<StockShared.Key, StockShared> stockProducer = new KafkaProducer<>(props, new JsonSerializer<>(StockShared.Key.class), new JsonSerializer<>(StockShared.class));
        ) {
            for (int wh = 0; wh < warehouseCount; wh++) {
                WarehouseShared warehouse = factory.makeWarehouseShared();
                warehouseProducer.send(new ProducerRecord<>(KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC, 0, warehouse.id, warehouse));

                DistrictFactory districtFactory = DistrictFactory.instanceFor(warehouse);
                for (int dst = 0; dst < districtsPerWarehouse; dst++) {
                    DistrictShared district = districtFactory.makeDistrictShared();
                    districtProducer.send(new ProducerRecord<>(KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, 0, district.getKey(), district));

                    CustomerFactory customerFactory = CustomerFactory.instanceFor(district);
                    for (int cust = 0; cust < customersPerDistrict; cust++) {
                        CustomerShared customer = customerFactory.makeCustomerShared();
                        customerProducer.send(new ProducerRecord<>(KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC, 0, customer.getKey(), customer));
                    }
                }

                StockFactory stockFactory = StockFactory.instanceFor(warehouse);
                for (int item = 0; item < itemCount; item++) {
                    StockShared stock = stockFactory.makeStockShared();
                    stockProducer.send(new ProducerRecord<>(KafkaConfig.STOCK_IMMUTABLE_TOPIC, 0, stock.getKey(), stock));
                }
            }
            warehouseProducer.flush();
        }
    }
}
