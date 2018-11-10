package net.jackw.olep.edge;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.JsonSerializer;
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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import static net.jackw.olep.common.KafkaConfig.TRANSACTION_EVENT_TOPIC;

/**
 * A connection to the OLEP database
 */
public class DatabaseConnection implements Closeable {
    private final Producer<String, TransactionRequestMessage> transactionEventProducer;
    private final Consumer<String, TransactionRequestMessage> transactionResultConsumer;

    private final Serializer<TransactionRequestMessage> transactionRequestSerializer = new JsonSerializer<>(TransactionRequestMessage.class);
    private final Deserializer<TransactionRequestMessage> transactionRequestDeserializer = new JsonDeserializer<>(TransactionRequestMessage.class);


    @MustBeClosed
    public DatabaseConnection(String bootstrapServers) {
        Properties transactionEventProducerProps = new Properties();
        transactionEventProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionEventProducer = new KafkaProducer<>(transactionEventProducerProps, Serdes.String().serializer(), transactionRequestSerializer);

        Properties transactionResultConsumerProps = new Properties();
        transactionResultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionResultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "result-consumer"); // TODO: this probably needs to be unique per app instance
        transactionResultConsumer = new KafkaConsumer<>(transactionResultConsumerProps, Serdes.String().deserializer(), transactionRequestDeserializer);
        transactionResultConsumer.subscribe(List.of(TRANSACTION_EVENT_TOPIC));
    }

    @Override
    public void close() {
        transactionEventProducer.close();
        transactionResultConsumer.close();
    }

    public void test(String body) {
        TestMessage msgBody = new TestMessage(body);
        send(msgBody);
    }

    private void send(TransactionRequestBody msgBody) {
        TransactionRequestMessage msg = new TransactionRequestMessage(msgBody);
        Future<RecordMetadata> result = transactionEventProducer.send(new ProducerRecord<>(TRANSACTION_EVENT_TOPIC, "test message", msg));
        // For now, do nothing
        // When result resolves, the data has been written out to Kafka
    }

    public void awaitRecord() {
        ConsumerRecords<String, TransactionRequestMessage> records = transactionResultConsumer.poll(Duration.ofSeconds(3));
        if (records.isEmpty()) {
            System.err.println("Failed to get records");
        } else {
            for (ConsumerRecord<String, TransactionRequestMessage> record: records) {
                String output;
                if (record.value().body instanceof TestMessage) {
                    TestMessage body = (TestMessage) record.value().body;
                    output = body.body;
                } else {
                    System.err.printf("Record body was of unexpected type %s", record.value().body.getClass().getName());
                    continue;
                }
                System.out.printf("%s: txid=%d, %s\n", record.key(), record.value().transactionId, output);
            }
        }
    }
}
