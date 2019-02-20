package net.jackw.olep.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.Futures;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.store.SharedCustomerStoreConsumer;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.store.SharedDistrictStoreConsumer;
import net.jackw.olep.common.store.SharedItemStoreConsumer;
import net.jackw.olep.common.store.SharedStockStoreConsumer;
import net.jackw.olep.common.store.SharedStoreConsumer;
import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.common.TransactionResultPartitioner;
import net.jackw.olep.common.store.SharedWarehouseStoreConsumer;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class WorkerApp extends StreamsApp {
    private SharedStoreConsumer<Integer, Item> itemConsumer;
    private SharedStoreConsumer<Integer, WarehouseShared> warehouseConsumer;
    private SharedStoreConsumer<WarehouseSpecificKey, DistrictShared> districtConsumer;
    private SharedCustomerStoreConsumer customerConsumer;
    private SharedStoreConsumer<WarehouseSpecificKey, StockShared> stockConsumer;

    private DatabaseConfig config;

    /**
     * A fake consumer, to allow access to {@link Consumer#partitionsFor(String)}
     */
    private Consumer<byte[], byte[]> pseudoConsumer;

    public WorkerApp(DatabaseConfig config) {
        super(config);

        this.config = config;

        // Consume from all the shared stores to make them accessible to workers
        String nodeId = getApplicationID() + "-" + getNodeID();
        itemConsumer = SharedItemStoreConsumer.create(getBootstrapServers(), nodeId, config);
        warehouseConsumer = SharedWarehouseStoreConsumer.create(getBootstrapServers(), nodeId, config);
        districtConsumer = SharedDistrictStoreConsumer.create(getBootstrapServers(), nodeId, config);
        customerConsumer = SharedCustomerStoreConsumer.create(getBootstrapServers(), nodeId, config);
        stockConsumer = SharedStockStoreConsumer.create(getBootstrapServers(), nodeId, config);

        Properties pseudoConsumerProperties = new Properties();
        pseudoConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        pseudoConsumer = new KafkaConsumer<>(
            pseudoConsumerProperties, Serdes.ByteArray().deserializer(), Serdes.ByteArray().deserializer()
        );
    }

    /**
     * Wait for all of the consumers to finish populating before starting to process items
     */
    @Override
    protected void beforeStart() throws InterruptedException, ExecutionException {
        Futures.allAsList(List.of(
            itemConsumer.getReadyFuture(), warehouseConsumer.getReadyFuture(), districtConsumer.getReadyFuture(),
            customerConsumer.getReadyFuture(), stockConsumer.getReadyFuture()
        )).get();
    }

    @Override
    public String getApplicationID() {
        return "worker";
    }

    @Override
    public void close() throws InterruptedException {
        super.close();
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

        StoreBuilder<KeyValueStore<DistrictSpecificKey, CustomerMutable>> customerMutableStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(KafkaConfig.CUSTOMER_MUTABLE_STORE),
            new JsonSerde<>(DistrictSpecificKey.class),
            new JsonSerde<>(CustomerMutable.class)
        );

        StoreBuilder<KeyValueStore<WarehouseSpecificKey, ArrayDeque<NewOrder>>> newOrdersStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(KafkaConfig.NEW_ORDER_STORE),
            new JsonSerde<>(WarehouseSpecificKey.class),
            new JsonSerde<>(new TypeReference<>() {})
        );

        StoreBuilder<KeyValueStore<WarehouseSpecificKey, Integer>> stockQuantityStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(KafkaConfig.STOCK_QUANTITY_STORE),
            new JsonSerde<>(WarehouseSpecificKey.class),
            Serdes.Integer()
        );

        Topology topology = new Topology();

        topology
            .addSource(
                Topology.AutoOffsetReset.EARLIEST,
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC,
                new TransactionWarehouseKey.KeyDeserializer(),
                new JsonDeserializer<>(TransactionRequestMessage.class),
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC
            )
            // Process takes candidate transactions, and decides whether they are acceptable
            .addProcessor("router", TransactionRouter::new, KafkaConfig.ACCEPTED_TRANSACTION_TOPIC)
            // Each transaction type is routed to a different processor
            .addProcessor("new-order-processor", () -> new NewOrderProcessor(
                itemConsumer.getStore(), warehouseConsumer.getStore(), districtConsumer.getStore(),
                customerConsumer.getStore(), stockConsumer.getStore(), config.getMetrics()
            ), "router")
            .addProcessor("payment-processor", () -> new PaymentProcessor(
                warehouseConsumer.getStore(), districtConsumer.getStore(), customerConsumer.getStore(), config.getMetrics()
            ), "router")
            .addProcessor("delivery-processor", () -> new DeliveryProcessor(config.getMetrics()), "router")
            // State stores for worker-local state
            .addStateStore(nextOrderIdStoreBuilder, "new-order-processor")
            .addStateStore(stockQuantityStoreBuilder, "new-order-processor")
            .addStateStore(newOrdersStoreBuilder, "new-order-processor", "delivery-processor")
            .addStateStore(customerMutableStoreBuilder, "payment-processor", "delivery-processor")
            // The processors will write to the result and modification logs
            .addSink(
                KafkaConfig.MODIFICATION_LOG,
                KafkaConfig.MODIFICATION_LOG,
                Serdes.Long().serializer(),
                new JsonSerializer<>(),
                "new-order-processor", "payment-processor", "delivery-processor"
            )
            .addSink(
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                new TransactionResultKey.ResultKeySerializer(),
                new JsonSerializer<>(),
                new TransactionResultPartitioner(),
                "new-order-processor", "payment-processor", "delivery-processor"
            );

        return topology;
    }

    @Override
    protected int getThreadCount() {
        return config.getWorkerThreads();
    }

    public static void main(String[] args) throws IOException {
        DatabaseConfig config = DatabaseConfig.create(args);
        run(config);
    }

    public static void run(DatabaseConfig config) {
        StreamsApp instance = new WorkerApp(config);
        instance.run();
    }
}
