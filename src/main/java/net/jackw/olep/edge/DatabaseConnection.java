package net.jackw.olep.edge;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.edge.transaction_result.TestResult;
import net.jackw.olep.edge.transaction_result.TransactionResult;
import net.jackw.olep.edge.transaction_result.TransactionResultBuilder;
import net.jackw.olep.edge.transaction_result.TransactionResultDeserializer;
import net.jackw.olep.message.TestMessage;
import net.jackw.olep.message.TransactionRequestBody;
import net.jackw.olep.message.TransactionRequestMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static net.jackw.olep.common.KafkaConfig.TRANSACTION_REQUEST_TOPIC;
import static net.jackw.olep.common.KafkaConfig.TRANSACTION_RESULT_TOPIC;

/**
 * A connection to the OLEP database
 */
public class DatabaseConnection implements Closeable {
    private final Producer<String, TransactionRequestMessage> transactionRequestProducer;
    private final Consumer<String, PendingTransaction> transactionResultConsumer;

    private final Serializer<TransactionRequestMessage> transactionRequestSerializer;
    private final Deserializer<PendingTransaction> transactionResultDeserializer;

    private final Map<Long, PendingTransaction<?, ?>> pendingTransactions;


    @MustBeClosed
    public DatabaseConnection(String bootstrapServers) {
        pendingTransactions = new ConcurrentHashMap<>();

        transactionRequestSerializer = new JsonSerializer<>(TransactionRequestMessage.class);
        Properties transactionRequestProducerProps = new Properties();
        transactionRequestProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionRequestProducerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        transactionRequestProducer = new KafkaProducer<>(transactionRequestProducerProps, Serdes.String().serializer(), transactionRequestSerializer);

        transactionResultDeserializer = new TransactionResultDeserializer(pendingTransactions);
        Properties transactionResultConsumerProps = new Properties();
        transactionResultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionResultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "result-consumer" + rand.nextInt()); // TODO: this probably needs to be unique per app instance
        transactionResultConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        transactionResultConsumer = new KafkaConsumer<>(transactionResultConsumerProps, Serdes.String().deserializer(), transactionResultDeserializer);
        transactionResultConsumer.subscribe(List.of(TRANSACTION_RESULT_TOPIC));
    }

    @Override
    public void close() {
        transactionRequestProducer.close();
        transactionResultConsumer.close();
    }

    /**
     * Send a test transaction
     *
     * @param body The message to send in the transaction
     */
    public TransactionStatus<TestResult> test(String body) {
        TestMessage msgBody = new TestMessage(body);
        return send(msgBody, TestResult.Builder.class).transactionStatus;
    }

    /**
     * Deliver a message to Kafka
     *
     * @param msgBody The message to send
     */
    private <T extends TransactionResult, B extends TransactionResultBuilder<T>> PendingTransaction<T, B> send(TransactionRequestBody msgBody, Class<B> resultBuilder) {
        long transactionId = nextTransactionId();

        PendingTransaction<T, B> pendingTransaction = new PendingTransaction<>(transactionId, resultBuilder);
        pendingTransactions.put(transactionId, pendingTransaction);

        TransactionRequestMessage msg = new TransactionRequestMessage(transactionId, msgBody);
        transactionRequestProducer.send(new ProducerRecord<>(TRANSACTION_REQUEST_TOPIC, "test message", msg), pendingTransaction.writtenToLogCallback);
        //transactionRequestProducer.flush();

        return pendingTransaction;
    }

    public void awaitRecord() {
        ConsumerRecords<String, PendingTransaction> records = transactionResultConsumer.poll(Duration.ofSeconds(3));
        if (records.isEmpty()) {
            System.err.println("Failed to get records");
        } else {
            for (ConsumerRecord<String, PendingTransaction> record: records) {
                System.out.printf("%s: txid=%d\n", record.key(), record.value().transactionId);
            }
        }
    }

    private static Random rand = new Random();

    /**
     * Generate a new transaction ID
     *
     * This has been abstracted into a method so different methods, such as snowflakes, can be used in future.
     *
     * @return A globally unique transaction ID
     */
    private long nextTransactionId() {
        return rand.nextLong();
    }
}
