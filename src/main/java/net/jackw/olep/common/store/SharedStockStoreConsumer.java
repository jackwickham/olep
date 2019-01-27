package net.jackw.olep.common.store;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.utils.populate.PredictableStockFactory;

public class SharedStockStoreConsumer extends SharedStoreConsumer<WarehouseSpecificKey, StockShared> {
    public SharedStockStoreConsumer(String bootstrapServers, String nodeID) {
        super(bootstrapServers, nodeID, KafkaConfig.STOCK_IMMUTABLE_TOPIC, WarehouseSpecificKey.class, StockShared.class);
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
}
