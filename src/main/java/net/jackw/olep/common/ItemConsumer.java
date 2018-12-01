package net.jackw.olep.common;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import net.jackw.olep.common.records.Item;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static net.jackw.olep.common.KafkaConfig.ITEM_IMMUTABLE_TOPIC;

public class ItemConsumer extends Thread implements AutoCloseable {
    private Consumer<Integer, Item> consumer;
    private SharedMapStore<Integer, Item> items;

    // I'm not sure if this flag really needs to be behind the lock, but it seems like the easiest way to make sure that
    // it will definitely be read correctly by the other thread, and the cost is very small because it is only used in
    // exceptional cases
    @GuardedBy("this")
    private boolean done = false;

    /**
     * Construct a new item consumer, and subscribe to the items log
     *
     * @param bootstrapServers The Kafka cluster's bootstrap servers
     * @param nodeID The ID of this node. It should be unique between all consumers of this log.
     */
    public ItemConsumer(String bootstrapServers, String nodeID) {
        items = new SharedMapStore<>(100_000);

        Properties itemConsumerProps = new Properties();
        itemConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        itemConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, nodeID);
        itemConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        itemConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer<>(itemConsumerProps, Serdes.Integer().deserializer(), new JsonDeserializer<>(Item.class));

        // We only subscribe to one partition here, because items should only ever have one partition
        TopicPartition partition = new TopicPartition(ITEM_IMMUTABLE_TOPIC, 0);
        consumer.assign(List.of(partition));
        consumer.seekToEnd(List.of(partition));
    }

    /**
     * Get the item store that is populated by this consumer
     */
    public SharedKeyValueStore<Integer, Item> getItems() {
        return items;
    }

    @Override
    public void run() {
        while (true) {
            try {
                ConsumerRecords<Integer, Item> receivedRecords = consumer.poll(Duration.ofHours(12));
                for (ConsumerRecord<Integer, Item> record : receivedRecords) {
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
