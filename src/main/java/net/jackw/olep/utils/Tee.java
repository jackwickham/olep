package net.jackw.olep.utils;

import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Simple log consumer
 */
public class Tee {
    public static void run() {
        Properties transactionResultConsumerProps = new Properties();
        transactionResultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        transactionResultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tee");
        transactionResultConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        Consumer<TransactionWarehouseKey, TransactionRequestMessage> consumer = new KafkaConsumer<>(
            transactionResultConsumerProps, new TransactionWarehouseKey.KeyDeserializer(), new JsonDeserializer<>(TransactionRequestMessage.class)
        );

        // Assign to the correct partition
        TopicPartition p1 = new TopicPartition(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, 0);
        TopicPartition p2 = new TopicPartition(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, 1);
        consumer.assign(List.of(p1, p2));
        consumer.seekToEnd(List.of(p1, p2));

        while (true) {
            ConsumerRecords<TransactionWarehouseKey, TransactionRequestMessage> msgs = consumer.poll(Duration.ofHours(12));
            for (ConsumerRecord<TransactionWarehouseKey, TransactionRequestMessage> msg : msgs) {
                System.out.printf("%d for %d: %s\n", msg.key().transactionId, msg.key().warehouseId, msg.value().toString());
            }
        }
    }

    public static void main(String[] args) {
        run();
    }
}
