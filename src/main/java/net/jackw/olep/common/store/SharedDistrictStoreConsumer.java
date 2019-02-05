package net.jackw.olep.common.store;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;

public class SharedDistrictStoreConsumer extends SharedStoreConsumer<WarehouseSpecificKey, DistrictShared> {
    private int referenceCount = 0;
    private InMemoryMapStore<WarehouseSpecificKey, DistrictShared> store;

    private SharedDistrictStoreConsumer(String bootstrapServers, String nodeId, DatabaseConfig config) {
        super(bootstrapServers, nodeId, KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, WarehouseSpecificKey.class, DistrictShared.class);
        store = new InMemoryMapStore<>(config.getWarehouseCount() * config.getItemCount());
    }

    @Override
    protected WritableKeyValueStore<WarehouseSpecificKey, DistrictShared> getWriteableStore() {
        return store;
    }

    private static SharedDistrictStoreConsumer instance;

    /**
     * Get a view on the store, creating it if it doesn't already exist
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param nodeId The ID for this processing node
     * @param config The current database configuration
     * @return A SharedWarehouseStore
     */
    public static synchronized SharedDistrictStoreConsumer create(String bootstrapServers, String nodeId, DatabaseConfig config) {
        if (instance == null) {
            instance = new SharedDistrictStoreConsumer(bootstrapServers, nodeId, config);
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
        synchronized (SharedDistrictStoreConsumer.class) {
            if (--referenceCount == 0) {
                instance = null;
                close = true;
            } else {
                close = false;
            }
        }

        if (close) {
            super.close();
        }
    }
}
