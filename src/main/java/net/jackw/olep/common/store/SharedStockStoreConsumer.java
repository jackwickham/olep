package net.jackw.olep.common.store;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.WarehouseSpecificKeySerde;
import net.jackw.olep.utils.populate.PredictableStockFactory;

public class SharedStockStoreConsumer extends SharedStoreConsumer<WarehouseSpecificKey, StockShared> {
    private int referenceCount = 0;
    private DiskBackedMapStore<WarehouseSpecificKey, StockShared> store;

    private SharedStockStoreConsumer(String bootstrapServers, String nodeId, DatabaseConfig config) {
        super(bootstrapServers, nodeId, KafkaConfig.STOCK_IMMUTABLE_TOPIC, WarehouseSpecificKeySerde.getInstance(), StockShared.class);
        store = DiskBackedMapStore.create(
            config.getWarehouseCount() * config.getItemCount(),
            WarehouseSpecificKey.class,
            StockShared.class,
            "stockshared",
            new WarehouseSpecificKey(1, 1),
            PredictableStockFactory.instanceFor(1).getStockShared(1),
            config,
            WarehouseSpecificKeySerde.getInstance()
        );
    }

    @Override
    protected WritableKeyValueStore<WarehouseSpecificKey, StockShared> getWriteableStore() {
        return store;
    }

    private static SharedStockStoreConsumer instance;

    /**
     * Get a view on the store, creating it if it doesn't already exist
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param nodeId The ID for this processing node
     * @param config The current database configuration
     * @return A SharedWarehouseStore
     */
    public static synchronized SharedStockStoreConsumer create(String bootstrapServers, String nodeId, DatabaseConfig config) {
        if (instance == null) {
            instance = new SharedStockStoreConsumer(bootstrapServers, nodeId, config);
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
        synchronized (SharedStockStoreConsumer.class) {
            if (--referenceCount == 0) {
                instance = null;
                close = true;
            } else {
                close = false;
            }
        }

        // Actually close it after releasing the lock, to avoid holding it unnecessarily long
        if (close) {
            super.close();
            store.close();
        }
    }
}
