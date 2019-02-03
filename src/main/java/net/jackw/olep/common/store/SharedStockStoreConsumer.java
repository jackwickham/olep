package net.jackw.olep.common.store;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.utils.populate.PredictableStockFactory;

public class SharedStockStoreConsumer extends SharedStoreConsumer<WarehouseSpecificKey, StockShared> {
    private int referenceCount = 0;

    private SharedStockStoreConsumer(String bootstrapServers, String nodeId) {
        super(bootstrapServers, nodeId, KafkaConfig.STOCK_IMMUTABLE_TOPIC, WarehouseSpecificKey.class, StockShared.class);
    }

    @Override
    protected WritableKeyValueStore<WarehouseSpecificKey, StockShared> createStore() {
        return DiskBackedMapStore.create(
            KafkaConfig.warehouseCount() * KafkaConfig.itemCount(),
            WarehouseSpecificKey.class,
            StockShared.class,
            "stockshared",
            new WarehouseSpecificKey(1, 1),
            PredictableStockFactory.instanceFor(1).getStockShared(1)
        );
    }

    private static SharedStockStoreConsumer instance;

    /**
     * Get a view on the store, creating it if it doesn't already exist
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param nodeId The ID for this processing node
     * @return A SharedWarehouseStore
     */
    public static synchronized SharedStockStoreConsumer create(String bootstrapServers, String nodeId) {
        if (instance == null) {
            instance = new SharedStockStoreConsumer(bootstrapServers, nodeId);
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
        synchronized (SharedStockStoreConsumer.class) {
            if (--referenceCount == 0) {
                super.close();
                instance = null;
            }
        }
    }
}
