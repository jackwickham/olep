package net.jackw.olep.worker;

import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewOrdersStore {
    private KeyValueStore<WarehouseSpecificKey, ArrayDeque<NewOrder>> store;

    /**
     * Construct a new instance from the underlying state store
     *
     * @param store The underlying Kafka KV store
     */
    public NewOrdersStore(KeyValueStore<WarehouseSpecificKey, ArrayDeque<NewOrder>> store) {
        this.store = store;
    }

    /**
     * Inserts the order as the most recent order in the specified district
     *
     * @param district The district that the order belongs to
     * @param order The newly created order's NewOrder representation
     */
    public void add(WarehouseSpecificKey district, NewOrder order) {
        ArrayDeque<NewOrder> queue = store.get(district);
        if (queue == null) {
            queue = new ArrayDeque<>(8);
        }
        queue.add(order);
        store.put(district, queue);
    }

    /**
     * Get the oldest order associated with the specified district
     *
     * @param district The district to look at
     * @return The oldest outstanding order associated with the district, or null if there are no outstanding orders
     */
    @Nullable
    public NewOrder poll(WarehouseSpecificKey district) {
        ArrayDeque<NewOrder> queue = store.get(district);
        if (queue == null || queue.isEmpty()) {
            return null;
        } else {
            NewOrder result = queue.remove();
            store.put(district, queue);
            return result;
        }
    }

    /**
     * Get the oldest order associated with each of the specified districts
     *
     * This method batches writes to rocksdb, which might make it perform better
     *
     * @param districts A list of districts to load the orders for
     * @return A map of district ID to the oldest new order associated with that district
     */
    public Map<WarehouseSpecificKey, NewOrder> pollAll(List<WarehouseSpecificKey> districts) {
        Map<WarehouseSpecificKey, NewOrder> results = new HashMap<>(districts.size());
        List<KeyValue<WarehouseSpecificKey, ArrayDeque<NewOrder>>> updatedQueues = new ArrayList<>(districts.size());
        for (WarehouseSpecificKey district : districts) {
            ArrayDeque<NewOrder> queue = store.get(district);
            if (queue != null && !queue.isEmpty()) {
                results.put(district, queue.remove());
                updatedQueues.add(new KeyValue<>(district, queue));
            }
        }
        if (!updatedQueues.isEmpty()) {
            store.putAll(updatedQueues);
        }
        return results;
    }
}
