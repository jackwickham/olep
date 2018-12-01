package net.jackw.olep.verifier;

import net.jackw.olep.StreamsApp;
import net.jackw.olep.common.ItemConsumer;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.message.TransactionRequestMessage;
import net.jackw.olep.message.TransactionResultMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class VerifierApp extends StreamsApp {
    private ItemConsumer itemConsumer;

    private VerifierApp(String bootstrapServers) {
        super(bootstrapServers);
        // Consume from items so we can check the transactions
        itemConsumer = new ItemConsumer(getBootstrapServers(), getApplicationID());
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
                "transaction-requests",
                Serdes.Long().deserializer(),
                new JsonDeserializer<>(TransactionRequestMessage.class),
                KafkaConfig.TRANSACTION_REQUEST_TOPIC
            )
            // Process takes candidate transactions, and decides whether they are acceptable
            .addProcessor("process", () -> new TransactionVerificationProcessor(itemConsumer.getItems()), "transaction-requests")
            .addSink(
                "accepted-transactions",
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC,
                Serdes.Long().serializer(),
                new JsonSerializer<>(TransactionRequestMessage.class),
                "process"
            )
            .addSink(
                "transaction-results",
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                Serdes.Long().serializer(),
                new JsonSerializer<>(TransactionResultMessage.class),
                "process"
            );

        return topology;
    }

    private StoreBuilder<KeyValueStore<Long, Item>> getItemStoreBuilder() {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("items"),
            Serdes.Long(),
            new JsonSerde<>(Item.class)
        );
    }

    public static void main(String[] args) {
        StreamsApp instance = new VerifierApp("localhost:9092");
        instance.run();
    }
}
