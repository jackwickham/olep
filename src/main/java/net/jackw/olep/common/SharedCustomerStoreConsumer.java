package net.jackw.olep.common;

import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;

public class SharedCustomerStoreConsumer extends SharedStoreConsumer<DistrictSpecificKey, CustomerShared> {
    /**
     * Construct a new shared store consumer, and subscribe to the corresponding topic
     *
     * @param bootstrapServers  The Kafka cluster's bootstrap servers
     * @param nodeID            The ID of this node. It should be unique between all consumers of this log.
     * @param topic             The changelog topic corresponding to this store
     */
    public SharedCustomerStoreConsumer(String bootstrapServers, String nodeID, String topic) {
        super(bootstrapServers, nodeID, topic, DistrictSpecificKey.class, CustomerShared.class);
    }

    /**
     * Create and return the underlying store that data should be saved in
     */
    @Override
    protected WritableKeyValueStore<DistrictSpecificKey, CustomerShared> createStore() {
        return new SharedCustomerMapStore(100_000);
    }

    /**
     * Get the store that is populated by this consumer
     */
    @Override
    public SharedCustomerStore getStore() {
        return (SharedCustomerStore) super.getStore();
    }
}
