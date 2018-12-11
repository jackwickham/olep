package net.jackw.olep.verifier;

import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.common.SharedStoreConsumer;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.TransactionResultPartitioner;
import net.jackw.olep.common.TransactionWarehouseKey;
import net.jackw.olep.common.WarehousePartitioner;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;

public class VerifierApp extends StreamsApp {
    private SharedStoreConsumer<Integer, Item> itemConsumer;

    private VerifierApp(String bootstrapServers) {
        super(bootstrapServers);

        // Consume from items so we can check the transactions
        itemConsumer = new SharedStoreConsumer<>(
            getBootstrapServers(),
            getApplicationID() + "-" + getNodeID(),
            KafkaConfig.ITEM_IMMUTABLE_TOPIC,
            Serdes.Integer().deserializer(),
            Item.class
        );
    }

    @Override
    public String getApplicationID() {
        return "verifier";
    }

    @Override
    protected void setup() {
        itemConsumer.start();
    }

    @Override
    protected void shutdown() throws InterruptedException {
        super.shutdown();
        itemConsumer.close();
    }

    @Override
    protected Topology getTopology() {
        Topology topology = new Topology();

        topology
            .addSource(
                Topology.AutoOffsetReset.EARLIEST,
                "transaction-requests",
                Serdes.Long().deserializer(),
                new JsonDeserializer<>(TransactionRequestMessage.class),
                KafkaConfig.TRANSACTION_REQUEST_TOPIC
            )
            // Process takes candidate transactions, and decides whether they are acceptable
            .addProcessor("process", () -> new TransactionVerificationProcessor(itemConsumer.getStore()), "transaction-requests")
            .addSink(
                "accepted-transactions",
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC,
                new TransactionWarehouseKey.KeySerializer(),
                new JsonSerializer<>(TransactionRequestMessage.class),
                new WarehousePartitioner(),
                "process"
            )
            .addSink(
                "transaction-results",
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                Serdes.Long().serializer(),
                new JsonSerializer<>(TransactionResultMessage.class),
                new TransactionResultPartitioner(),
                "process"
            );

        return topology;
    }

    public static void main(String[] args) {
        StreamsApp instance = new VerifierApp("localhost:9092");
        instance.run();
    }
}
