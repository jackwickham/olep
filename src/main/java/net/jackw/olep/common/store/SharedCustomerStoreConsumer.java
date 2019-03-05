package net.jackw.olep.common.store;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.DistrictSpecificKeySerde;

public class SharedCustomerStoreConsumer extends SharedStoreConsumer<DistrictSpecificKey, CustomerShared> {
    private int referenceCount = 0;
    private DiskBackedCustomerMapStore store;

    /**
     * Construct a new shared store consumer, and subscribe to the corresponding topic
     *
     * @param bootstrapServers  The Kafka cluster's bootstrap servers
     * @param nodeId            The ID of this node. It should be unique between all consumers of this log.
     * @param config            The current database config
     */
    private SharedCustomerStoreConsumer(String bootstrapServers, String nodeId, DatabaseConfig config) {
        super(bootstrapServers, nodeId, KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC, DistrictSpecificKeySerde.getInstance(), CustomerShared.class);
        store = new DiskBackedCustomerMapStore(config);
    }

    /**
     * Create and return the underlying store that data should be saved in
     */
    @Override
    protected WritableKeyValueStore<DistrictSpecificKey, CustomerShared> getWriteableStore() {
        return store;
    }

    /**
     * Get the store that is populated by this consumer
     */
    @Override
    public SharedCustomerStore getStore() {
        return (SharedCustomerStore) super.getStore();
    }

    private static SharedCustomerStoreConsumer instance;

    /**
     * Get a view on the store, creating it if it doesn't already exist
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param nodeId The ID for this processing node
     * @param config The current database config
     * @return A SharedWarehouseStore
     */
    public static synchronized SharedCustomerStoreConsumer create(String bootstrapServers, String nodeId, DatabaseConfig config) {
        if (instance == null) {
            instance = new SharedCustomerStoreConsumer(bootstrapServers, nodeId, config);
            instance.start();
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
        boolean close;
        synchronized (SharedCustomerStoreConsumer.class) {
            if (--referenceCount == 0) {
                close = true;
                instance = null;
            } else {
                close = false;
            }
        }

        if (close) {
            super.close();
            store.close();
        }
    }
}
