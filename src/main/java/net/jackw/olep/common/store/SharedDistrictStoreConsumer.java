package net.jackw.olep.common.store;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.utils.populate.PredictableDistrictFactory;

public class SharedDistrictStoreConsumer extends SharedStoreConsumer<WarehouseSpecificKey, DistrictShared> {
    public SharedDistrictStoreConsumer(String bootstrapServers, String nodeID) {
        super(bootstrapServers, nodeID, KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, WarehouseSpecificKey.class, DistrictShared.class);
    }

    @Override
    protected WritableKeyValueStore<WarehouseSpecificKey, DistrictShared> createStore() {
        return DiskBackedMapStore.create(
            KafkaConfig.warehouseCount() * KafkaConfig.itemCount(),
            WarehouseSpecificKey.class,
            DistrictShared.class,
            "districtshared",
            new WarehouseSpecificKey(1, 1),
            PredictableDistrictFactory.instanceFor(1).getDistrictShared(1)
        );
    }
}
