package net.jackw.olep.common.store;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.utils.populate.PredictableItemFactory;
import org.apache.kafka.common.serialization.Serdes;

public class SharedItemStoreConsumer extends SharedStoreConsumer<Integer, Item> {
    public SharedItemStoreConsumer(String bootstrapServers, String nodeID) {
        super(
            bootstrapServers, nodeID, KafkaConfig.ITEM_IMMUTABLE_TOPIC, Serdes.Integer().deserializer(),
            Item.class
        );
    }

    @Override
    protected WritableKeyValueStore<Integer, Item> createStore() {
        return DiskBackedMapStore.createIntegerKeyed(
            KafkaConfig.warehouseCount() * KafkaConfig.itemCount(),
            Item.class,
            "itemshared",
            PredictableItemFactory.getInstance().getItem(1)
        );
    }
}
