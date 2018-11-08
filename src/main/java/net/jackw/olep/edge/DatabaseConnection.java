package net.jackw.olep.edge;

import com.google.errorprone.annotations.MustBeClosed;
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
import org.apache.kafka.common.serialization.Serdes;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * A connection to the OLEP database
 */
public class DatabaseConnection implements Closeable {
    private final Producer<String, String> transactionEventProducer;
    private final Consumer<String, String> transactionResultConsumer;

    // TODO: Move these to a separate shared class
    private final String TRANSACTION_EVENT_TOPIC = "transaction-input";
    private final String TRANSACTION_RESULT_TOPIC = "transaction-result";

    @MustBeClosed
    public DatabaseConnection(String bootstrapServers) {
        Properties transactionEventProducerProps = new Properties();
        transactionEventProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionEventProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        transactionEventProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        transactionEventProducer = new KafkaProducer<>(transactionEventProducerProps);

        Properties transactionResultConsumerProps = new Properties();
        transactionResultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionResultConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        transactionResultConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        transactionResultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "result-consumer"); // TODO: this probably needs to be unique per app instance
        transactionResultConsumer = new KafkaConsumer<>(transactionResultConsumerProps);
        transactionResultConsumer.subscribe(List.of(TRANSACTION_EVENT_TOPIC));
    }

    @Override
    public void close() {
        transactionEventProducer.close();
        transactionResultConsumer.close();
    }

    public void test(String body) {
        Future<RecordMetadata> result = transactionEventProducer.send(new ProducerRecord<>(TRANSACTION_EVENT_TOPIC, "test message", body));
        // For now, do nothing
    }

    public void awaitRecord() {
        ConsumerRecords<String, String> records = transactionResultConsumer.poll(Duration.ofSeconds(3));
        if (records.isEmpty()) {
            System.err.println("Failed to get records");
        } else {
            for (ConsumerRecord<String, String> record: records) {
                System.out.printf("%s: %s\n", record.key(), record.value());
            }
        }
    }
}
