package net.jackw.olep.verifier;

import com.google.common.util.concurrent.ListenableFuture;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class VerifierApp extends StreamsApp {
    private SharedStoreConsumer<Integer, Item> itemConsumer;
    private DatabaseConfig config;

    public VerifierApp(DatabaseConfig config) {
        super(config);
        this.config = config;

        // Consume from items so we can check the transactions
        itemConsumer = SharedItemStoreConsumer.create(
            getBootstrapServers(), getApplicationID() + "-" + getNodeID(), config
        );
    }

    /**
     * Wait for the items store to be fully populated before starting
     */
    @Override
    protected ListenableFuture<Void> getBeforeStartFuture() {
        return itemConsumer.getReadyFuture();
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
            .addProcessor(
                "process",
                () -> new TransactionVerificationProcessor(itemConsumer.getStore(), config.getMetrics()),
                KafkaConfig.TRANSACTION_REQUEST_TOPIC
            )
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

    @Override
    protected int getThreadCount() {
        return config.getVerifierThreads();
    }

    @Override
    protected int getStateStoreCount() {
        return 0;
    }

    @Override
    protected Properties getStreamProperties() {
        Properties props = super.getStreamProperties();
        // Buffer verification results for up to 2ms and 64KiB
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 2);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 65536);
        return props;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Arguments arguments = new Arguments(args);
        StreamsApp instance = new VerifierApp(arguments.getConfig());
        instance.runForever(arguments.getReadyFileArg());
    }
}
