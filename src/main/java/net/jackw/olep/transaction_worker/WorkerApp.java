package net.jackw.olep.transaction_worker;

import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.SharedStoreConsumer;
import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;

public class WorkerApp extends StreamsApp {
    private SharedStoreConsumer<Integer, Item> itemConsumer;
    private SharedStoreConsumer<Integer, WarehouseShared> warehouseConsumer;
    private SharedStoreConsumer<DistrictShared.Key, DistrictShared> districtConsumer;
    private SharedStoreConsumer<CustomerShared.Key, CustomerShared> customerConsumer;
    private SharedStoreConsumer<StockShared.Key, StockShared> stockConsumer;

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
            DistrictShared.Key.class,
            DistrictShared.class
        );
        customerConsumer = new SharedStoreConsumer<>(
            getBootstrapServers(),
            getApplicationID() + "-" + getNodeID(),
            KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC,
            CustomerShared.Key.class,
            CustomerShared.class
        );
        stockConsumer = new SharedStoreConsumer<>(
            getBootstrapServers(),
            getApplicationID() + "-" + getNodeID(),
            KafkaConfig.STOCK_IMMUTABLE_TOPIC,
            StockShared.Key.class,
            StockShared.class
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
    }

    @Override
    protected Topology getTopology() {
        Topology topology = new Topology();

        topology
            .addSource(
                "accepted-transactions",
                Serdes.Long().deserializer(),
                new JsonDeserializer<>(TransactionRequestMessage.class),
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC
            )
            // Process takes candidate transactions, and decides whether they are acceptable
            .addProcessor("router", TransactionRouter::new, "accepted-transactions")
            // Each transaction type is routed to a different processor
            .addProcessor("new-order-processor", () -> new NewOrderProcessor(
                itemConsumer.getStore(), warehouseConsumer.getStore(), districtConsumer.getStore(),
                customerConsumer.getStore(), stockConsumer.getStore()
            ), "router")
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
                "new-order-processor"
            );

        return topology;
    }

    public static void main(String[] args) {
        StreamsApp instance = new WorkerApp("localhost:9092");
        instance.run();
    }
}
