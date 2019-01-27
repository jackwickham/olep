package net.jackw.olep.common.store;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;

public class SharedDistrictStoreConsumer extends SharedStoreConsumer<WarehouseSpecificKey, DistrictShared> {
    public SharedDistrictStoreConsumer(String bootstrapServers, String nodeID) {
        super(bootstrapServers, nodeID, KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, WarehouseSpecificKey.class, DistrictShared.class);
    }

    @Override
    protected WritableKeyValueStore<WarehouseSpecificKey, DistrictShared> createStore() {
        return new InMemoryMapStore<>(KafkaConfig.warehouseCount() * KafkaConfig.itemCount());
    }
}
