package net.jackw.olep.verifier;

import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.common.store.SharedItemStoreConsumer;
import net.jackw.olep.common.store.SharedStoreConsumer;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.TransactionResultPartitioner;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.common.WarehousePartitioner;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;

public class VerifierApp extends StreamsApp {
    private SharedStoreConsumer<Integer, Item> itemConsumer;

    public VerifierApp(String bootstrapServers) {
        super(bootstrapServers);

        // Consume from items so we can check the transactions
        itemConsumer = SharedItemStoreConsumer.create(getBootstrapServers(), getApplicationID() + "-" + getNodeID());
    }

    @Override
    public String getApplicationID() {
        return "verifier";
    }

    @Override
    public void close() throws InterruptedException {
        super.close();
        itemConsumer.close();
    }

    @Override
    protected Topology getTopology() {
        Topology topology = new Topology();

        topology
            .addSource(
                Topology.AutoOffsetReset.EARLIEST,
                KafkaConfig.TRANSACTION_REQUEST_TOPIC,
                Serdes.Long().deserializer(),
                new JsonDeserializer<>(TransactionRequestMessage.class),
                KafkaConfig.TRANSACTION_REQUEST_TOPIC
            )
            // Process takes candidate transactions, and decides whether they are acceptable
            .addProcessor("process", () -> new TransactionVerificationProcessor(itemConsumer.getStore()), KafkaConfig.TRANSACTION_REQUEST_TOPIC)
            .addSink(
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC,
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC,
                new TransactionWarehouseKey.KeySerializer(),
                new JsonSerializer<>(),
                new WarehousePartitioner(),
                "process"
            )
            .addSink(
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                new TransactionResultKey.ResultKeySerializer(),
                new JsonSerializer<>(),
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
