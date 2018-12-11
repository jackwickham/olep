package net.jackw.olep.transaction_worker;

import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.SharedStoreConsumer;
import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.common.TransactionResultPartitioner;
import net.jackw.olep.common.TransactionWarehouseKey;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class WorkerApp extends StreamsApp {
    private SharedStoreConsumer<Integer, Item> itemConsumer;
    private SharedStoreConsumer<Integer, WarehouseShared> warehouseConsumer;
    private SharedStoreConsumer<WarehouseSpecificKey, DistrictShared> districtConsumer;
    private SharedStoreConsumer<DistrictSpecificKey, CustomerShared> customerConsumer;
    private SharedStoreConsumer<WarehouseSpecificKey, StockShared> stockConsumer;

    /**
     * A fake consumer, to allow access to {@link Consumer#partitionsFor(String)}
     */
    private Consumer<byte[], byte[]> pseudoConsumer;

    private WorkerApp(String bootstrapServers) {
        super(bootstrapServers);

        // Consume from all the shared stores to make them accessible to workers
        itemConsumer = new SharedStoreConsumer<>(
            getBootstrapServers(),
            getApplicationID() + "-" + getNodeID(),
            KafkaConfig.ITEM_IMMUTABLE_TOPIC,
            Serdes.Integer().deserializer(),
            Item.class
        );
        warehouseConsumer = new SharedStoreConsumer<>(
            getBootstrapServers(),
            getApplicationID() + "-" + getNodeID(),
            KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC,
            Serdes.Integer().deserializer(),
            WarehouseShared.class
        );
        districtConsumer = new SharedStoreConsumer<>(
            getBootstrapServers(),
            getApplicationID() + "-" + getNodeID(),
            KafkaConfig.DISTRICT_IMMUTABLE_TOPIC,
            WarehouseSpecificKey.class,
            DistrictShared.class
        );
        customerConsumer = new SharedStoreConsumer<>(
            getBootstrapServers(),
            getApplicationID() + "-" + getNodeID(),
            KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC,
            DistrictSpecificKey.class,
            CustomerShared.class
        );
        stockConsumer = new SharedStoreConsumer<>(
            getBootstrapServers(),
            getApplicationID() + "-" + getNodeID(),
            KafkaConfig.STOCK_IMMUTABLE_TOPIC,
            WarehouseSpecificKey.class,
            StockShared.class
        );

        Properties pseudoConsumerProperties = new Properties();
        pseudoConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        pseudoConsumer = new KafkaConsumer<>(
            pseudoConsumerProperties, Serdes.ByteArray().deserializer(), Serdes.ByteArray().deserializer()
        );
    }

    @Override
    public String getApplicationID() {
        return "worker";
    }

    @Override
    protected void setup() {
        itemConsumer.start();
        warehouseConsumer.start();
        districtConsumer.start();
        customerConsumer.start();
        stockConsumer.start();
    }

    @Override
    protected void shutdown() throws InterruptedException {
        super.shutdown();
        itemConsumer.close();
        warehouseConsumer.close();
        districtConsumer.close();
        customerConsumer.close();
        stockConsumer.close();
        pseudoConsumer.close();
    }

    @Override
    protected Topology getTopology() {
        StoreBuilder<KeyValueStore<WarehouseSpecificKey, Integer>> nextOrderIdStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(KafkaConfig.DISTRICT_NEXT_ORDER_ID_STORE),
            new JsonSerde<>(WarehouseSpecificKey.class),
            Serdes.Integer()
        );

        //StoreBuilder<KeyValueStore<CustomerShared.Key, CustomerMutable>> customerMutableStoreBuilder

        // customer_id_immutable probably doesn't belong here, and new_orders doesn't either

        StoreBuilder<KeyValueStore<WarehouseSpecificKey, Integer>> stockQuantityStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(KafkaConfig.STOCK_QUANTITY_STORE),
            new JsonSerde<>(WarehouseSpecificKey.class),
            Serdes.Integer()
        );

        final int acceptedTransactionsPartitions = pseudoConsumer.partitionsFor(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC)
            .size();

        Topology topology = new Topology();

        topology
            .addSource(
                Topology.AutoOffsetReset.EARLIEST,
                "accepted-transactions",
                new TransactionWarehouseKey.KeyDeserializer(),
                new JsonDeserializer<>(TransactionRequestMessage.class),
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC
            )
            // Process takes candidate transactions, and decides whether they are acceptable
            .addProcessor("router", TransactionRouter::new, "accepted-transactions")
            // Each transaction type is routed to a different processor
            .addProcessor("new-order-processor", () -> new NewOrderProcessor(
                itemConsumer.getStore(), warehouseConsumer.getStore(), districtConsumer.getStore(),
                customerConsumer.getStore(), stockConsumer.getStore(), acceptedTransactionsPartitions
            ), "router")
            // State stores for worker-local state
            .addStateStore(nextOrderIdStoreBuilder, "new-order-processor")
            .addStateStore(stockQuantityStoreBuilder, "new-order-processor")
            // The processors will write to the result and modification logs
            .addSink(
                "modification-log",
                KafkaConfig.MODIFICATION_LOG,
                Serdes.Long().serializer(),
                new JsonSerializer<>(Object.class), // TODO: Consider what should be written to the modification log
                "new-order-processor"
            )
            .addSink(
                "transaction-results",
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                Serdes.Long().serializer(),
                new JsonSerializer<>(TransactionResultMessage.class),
                new TransactionResultPartitioner(),
                "new-order-processor"
            );

        return topology;
    }

    public static void main(String[] args) {
        StreamsApp instance = new WorkerApp("localhost:9092");
        instance.run();
    }
}
