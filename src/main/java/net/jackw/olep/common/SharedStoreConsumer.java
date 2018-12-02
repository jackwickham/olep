package net.jackw.olep.common;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Consumer for a shared key-value store, manually implemented with a Kafka changelog topic
 *
 * @param <K> The store key
 * @param <V> The store value
 */
public class SharedStoreConsumer<K, V> extends Thread implements AutoCloseable {
    private Consumer<K, V> consumer;
    private SharedMapStore<K, V> items;

    // I'm not sure if this flag really needs to be behind the lock, but it seems like the easiest way to make sure that
    // it will definitely be read correctly by the other thread, and the cost is very small because it is only used in
    // exceptional cases
    @GuardedBy("this")
    private boolean done = false;

    /**
     * Construct a new shared store consumer, and subscribe to the corresponding log
     *
     * @param bootstrapServers The Kafka cluster's bootstrap servers
     * @param nodeID The ID of this node. It should be unique between all consumers of this log.
     */
    public SharedStoreConsumer(String bootstrapServers, String nodeID, Class<V> valueClass, String log, Deserializer<K> keyDeserializer) {
        items = new SharedMapStore<>(100_000);

        Properties itemConsumerProps = new Properties();
        itemConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        itemConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, nodeID);
        itemConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        itemConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer<>(itemConsumerProps, keyDeserializer, new JsonDeserializer<>(valueClass));

        // We only subscribe to one partition here, because items should only ever have one partition
        TopicPartition partition = new TopicPartition(log, 0);
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
    }

    /**
     * Get the store that is populated by this consumer
     */
    public SharedKeyValueStore<K, V> getItems() {
        return items;
    }

    @Override
    public void run() {
        while (true) {
            try {
                ConsumerRecords<K, V> receivedRecords = consumer.poll(Duration.ofHours(12));
                for (ConsumerRecord<K, V> record : receivedRecords) {
                    if (record.value() == null) {
                        items.remove(record.key());
                    } else {
                        items.put(record.key(), record.value());
                    }
                }
            } catch (WakeupException e) {
                synchronized (this) {
                    if (done) {
                        return;
                    }
                }
            }
        }
    }

    @Override
    public void close() throws InterruptedException {
        synchronized (this) {
            done = true;
        }
        consumer.wakeup();
        join();
    }
}
