package net.jackw.olep.common.store;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.utils.populate.PredictableItemFactory;
import org.apache.kafka.common.serialization.Serdes;

public class SharedItemStoreConsumer extends SharedStoreConsumer<Integer, Item> {
    private int referenceCount = 0;
    private InMemoryMapStore<Integer, Item> store;

    private SharedItemStoreConsumer(String bootstrapServers, String nodeId, DatabaseConfig config) {
        super(
            bootstrapServers, nodeId, KafkaConfig.ITEM_IMMUTABLE_TOPIC, Serdes.Integer().deserializer(),
            Item.class
        );
        store = new InMemoryMapStore<>(config.getWarehouseCount() * config.getItemCount());
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
     * @param config The current database configuration
     * @return A SharedWarehouseStore
     */
    public static synchronized SharedItemStoreConsumer create(String bootstrapServers, String nodeId, DatabaseConfig config) {
        if (instance == null) {
            instance = new SharedItemStoreConsumer(bootstrapServers, nodeId, config);
            instance.start();
        }
        instance.referenceCount++;
        return instance;
    }

    /**
     * Decrement the reference count, closing the underlying store if there are no remaining references.
     *
     * This method must be called once per call to {@link #create(String, String, DatabaseConfig)}.
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

        if (close) {
            super.close();
        }
    }
}
