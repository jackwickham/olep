package net.jackw.olep.common.store;

import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;

public class SharedCustomerStoreConsumer extends SharedStoreConsumer<DistrictSpecificKey, CustomerShared> {
    /**
     * Construct a new shared store consumer, and subscribe to the corresponding topic
     *
     * @param bootstrapServers  The Kafka cluster's bootstrap servers
     * @param nodeID            The ID of this node. It should be unique between all consumers of this log.
     */
    public SharedCustomerStoreConsumer(String bootstrapServers, String nodeID) {
        super(bootstrapServers, nodeID, KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC, DistrictSpecificKey.class, CustomerShared.class);
    }

    /**
     * Create and return the underlying store that data should be saved in
     */
    @Override
    protected WritableKeyValueStore<DistrictSpecificKey, CustomerShared> createStore() {
        return new DiskBackedCustomerMapStore();
    }

    /**
     * Get the store that is populated by this consumer
     */
    @Override
    public SharedCustomerStore getStore() {
        return (SharedCustomerStore) super.getStore();
    }
}
