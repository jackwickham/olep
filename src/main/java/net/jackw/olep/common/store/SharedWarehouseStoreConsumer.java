package net.jackw.olep.common.store;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.WarehouseShared;
import org.apache.kafka.common.serialization.Serdes;

public class SharedWarehouseStoreConsumer extends SharedStoreConsumer<Integer, WarehouseShared> {
    public SharedWarehouseStoreConsumer(String bootstrapServers, String nodeID) {
        super(
            bootstrapServers, nodeID, KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC, Serdes.Integer().deserializer(),
            WarehouseShared.class
        );
    }

    @Override
    protected WritableKeyValueStore<Integer, WarehouseShared> createStore() {
        return new InMemoryMapStore<>(KafkaConfig.warehouseCount() * KafkaConfig.itemCount());
    }
}
