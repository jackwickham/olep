package net.jackw.olep.utils.immutable_stores;

import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.Item;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

@SuppressWarnings("FutureReturnValueIgnored")
public class PopulateStores {
    private String bootstrapServers = "localhost:9092";

    public static void main(String[] args) {
        PopulateStores i = new PopulateStores();
        i.populateItems(100);
    }

    private void populateItems(int itemCount) {
        ItemFactory factory = ItemFactory.getInstance();

        // Set up the producer, which is used to send requests from the application to the DB
        Serializer<Item> serializer = new JsonSerializer<>(Item.class);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);

        try (Producer<Integer, Item> producer = new KafkaProducer<>(props, Serdes.Integer().serializer(), serializer)) {
            for (int i = 0; i < itemCount; i++) {
                Item item = factory.makeItem();
                producer.send(new ProducerRecord<>(KafkaConfig.ITEM_IMMUTABLE_TOPIC, 0, item.id, item));
            }
            producer.flush();
        }
    }
}
