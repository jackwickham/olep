package net.jackw.olep.utils;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simple log consumer
 */
public class Tee {
    public static void run() {
        Properties transactionResultConsumerProps = new Properties();
        transactionResultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        transactionResultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tee");
        transactionResultConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        Consumer<TransactionResultKey, byte[]> consumer = new KafkaConsumer<>(
            transactionResultConsumerProps, new TransactionResultKey.ResultKeyDeserializer(), Serdes.ByteArray().deserializer()
        );

        // Assign to the correct partition
        TopicPartition p1 = new TopicPartition(KafkaConfig.TRANSACTION_RESULT_TOPIC, 0);
        TopicPartition p2 = new TopicPartition(KafkaConfig.TRANSACTION_RESULT_TOPIC, 1);
        consumer.assign(List.of(p1, p2));
        consumer.seekToEnd(List.of(p1, p2));

        while (true) {
            ConsumerRecords<TransactionResultKey, byte[]> msgs = consumer.poll(Duration.ofHours(12));
            for (ConsumerRecord<TransactionResultKey, byte[]> msg : msgs) {
                System.out.printf("%d is approval: %s: %s\n", msg.key().transactionId, msg.key().approvalMessage ? "true": "false", new String(msg.value(), UTF_8));
            }
        }
    }

    public static void main(String[] args) {
        run();
    }
}
