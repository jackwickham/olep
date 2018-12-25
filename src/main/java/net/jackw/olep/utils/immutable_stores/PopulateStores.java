package net.jackw.olep.utils.immutable_stores;

import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.view.RedisAdapter;
import net.jackw.olep.view.ViewWriteAdapter;
import net.jackw.olep.view.records.Customer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.math.BigDecimal;
import java.util.Properties;

@SuppressWarnings("FutureReturnValueIgnored")
public class PopulateStores {
    private String bootstrapServers = "localhost:9092";
    private Properties props;
    private String redisHost = "localhost";

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
        Serializer<Item> serializer = new JsonSerializer<>();

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
            Producer<Integer, WarehouseShared> warehouseProducer = new KafkaProducer<>(
                props, Serdes.Integer().serializer(), new JsonSerializer<>()
            );
            Producer<WarehouseSpecificKey, DistrictShared> districtProducer = new KafkaProducer<>(
                props, new JsonSerializer<>(), new JsonSerializer<>()
            );
            Producer<DistrictSpecificKey, CustomerShared> customerProducer = new KafkaProducer<>(
                props, new JsonSerializer<>(), new JsonSerializer<>()
            );
            Producer<WarehouseSpecificKey, StockShared> stockProducer = new KafkaProducer<>(
                props, new JsonSerializer<>(), new JsonSerializer<>()
            );
            ViewWriteAdapter viewWriteAdapter = new RedisAdapter(redisHost);
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

                        Customer viewCustomer = new Customer(
                            customer.id, customer.districtId, customer.warehouseId, customer.firstName, customer.middleName,
                            customer.lastName, new BigDecimal("-10"), 0, 1, null,
                            ImmutableList.of()
                        );
                        viewWriteAdapter.addCustomer(viewCustomer);
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
