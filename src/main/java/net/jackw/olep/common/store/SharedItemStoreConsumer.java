package net.jackw.olep.common.store;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.utils.populate.PredictableItemFactory;
import org.apache.kafka.common.serialization.Serdes;

public class SharedItemStoreConsumer extends SharedStoreConsumer<Integer, Item> {
    private int referenceCount = 0;
    private DiskBackedMapStore<Integer, Item> store;

    private SharedItemStoreConsumer(String bootstrapServers, String nodeId) {
        super(
            bootstrapServers, nodeId, KafkaConfig.ITEM_IMMUTABLE_TOPIC, Serdes.Integer().deserializer(),
            Item.class
        );
        store = DiskBackedMapStore.createIntegerKeyed(
            KafkaConfig.warehouseCount() * KafkaConfig.itemCount(),
            Item.class,
            "itemshared",
            PredictableItemFactory.getInstance().getItem(1)
        );
    }

    @Override
    protected WritableKeyValueStore<Integer, Item> getWriteableStore() {
        return store;
    }

    private static SharedItemStoreConsumer instance;

    /**
     * Get a view on the store, creating it if it doesn't already exist
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param nodeId The ID for this processing node
     * @return A SharedWarehouseStore
     */
    public static SharedItemStoreConsumer create(String bootstrapServers, String nodeId) {
        if (instance == null) {
            instance = new SharedItemStoreConsumer(bootstrapServers, nodeId);
            instance.start();
        }
        instance.referenceCount++;
        return instance;
    }

    /**
     * Decrement the reference count, closing the underlying store if there are no remaining references.
     *
     * This method must be called once per call to {@link #create(String, String)}.
     */
    @Override
    public void close() throws InterruptedException {
        boolean close;
        synchronized (SharedItemStoreConsumer.class) {
            if (--referenceCount == 0) {
                close = true;
                instance = null;
            } else {
                close = false;
            }
        }

        // Closing the disk store might be slow, so release the lock first
        if (close) {
            super.close();
            store.close();
        }
    }
}
