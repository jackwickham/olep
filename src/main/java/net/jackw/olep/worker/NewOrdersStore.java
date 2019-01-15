package net.jackw.olep.worker;

import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.annotation.Nullable;
import java.util.ArrayDeque;

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
}
