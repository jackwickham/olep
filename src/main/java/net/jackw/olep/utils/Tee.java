package net.jackw.olep.utils;

import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simple log consumer
 */
public class Tee {
    public static void run(DatabaseConfig config) {
        Properties transactionResultConsumerProps = new Properties();
        transactionResultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        transactionResultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tee");
        transactionResultConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<TransactionResultKey, byte[]> consumer = new KafkaConsumer<>(
            transactionResultConsumerProps, new TransactionResultKey.ResultKeyDeserializer(), Serdes.ByteArray().deserializer()
        );

        // Assign to the correct partition
        ArrayList<TopicPartition> partitions = new ArrayList<>(config.getTransactionResultTopicPartitions());
        for (int i = 0; i < config.getTransactionResultTopicPartitions(); i++) {
            partitions.add(new TopicPartition(KafkaConfig.TRANSACTION_RESULT_TOPIC, 0));
        }
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        // Populate this:
        Set<Long> interestingTransactions = Set.of();

        while (true) {
            ConsumerRecords<TransactionResultKey, byte[]> msgs = consumer.poll(Duration.ofSeconds(5));
            if (msgs.isEmpty()) {
                break;
            }
            for (ConsumerRecord<TransactionResultKey, byte[]> msg : msgs) {
                if (interestingTransactions.contains(msg.key().transactionId)) {
                    if (msg.key().approvalMessage) {
                        System.out.printf(
                            "%d: Approval of %d: %s\n",
                            msg.timestamp(),
                            msg.key().transactionId,
                            new String(msg.value(), UTF_8)
                        );
                    } else {
                        System.out.printf(
                            "%d: Result for %d: %s\n",
                            msg.timestamp(),
                            msg.key().transactionId,
                            new String(msg.value(), UTF_8)
                        );
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Arguments arguments = new Arguments(args);
        run(arguments.getConfig());
    }
}
