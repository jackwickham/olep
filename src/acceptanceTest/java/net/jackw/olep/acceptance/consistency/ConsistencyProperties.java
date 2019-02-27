package net.jackw.olep.acceptance.consistency;

import net.jackw.olep.acceptance.CurrentTestState;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayDeque;

import static org.junit.Assert.*;

/**
 * TPC-C §3.3
 */
public abstract class ConsistencyProperties {
    @BeforeClass
    public static void checkPartOfSuite() {
        Assume.assumeTrue(CurrentTestState.hasInstance());
    }

    // 3.3.2.1 doesn't apply

    /**
     * §3.3.2.2
     * Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
     *     D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
     * for each district defined by (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID). This condition does not
     * apply to the NEW-ORDER table for any districts which have no outstanding new orders (i.e., the number of rows is
     * zero).
     */
    @Test
    public void testNextOrderIdMatchesGreatestNewOrderId() {
        KafkaStreams streams = CurrentTestState.getInstance().workerApp.getStreams();
        ReadOnlyKeyValueStore<WarehouseSpecificKey, Integer> nextOrderIdStore =
            streams.store(KafkaConfig.DISTRICT_NEXT_ORDER_ID_STORE, QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<WarehouseSpecificKey, ArrayDeque<NewOrder>> newOrderStore =
            streams.store(KafkaConfig.NEW_ORDER_STORE, QueryableStoreTypes.keyValueStore());

        for (int warehouseId = 1; warehouseId <= CurrentTestState.getInstance().config.getWarehouseCount(); warehouseId++) {
            for (int districtId = 1; districtId <= CurrentTestState.getInstance().config.getDistrictsPerWarehouse(); districtId++) {
                WarehouseSpecificKey districtKey = new WarehouseSpecificKey(districtId, warehouseId);

                int expected = nextOrderIdStore.get(districtKey) - 1;
                ArrayDeque<NewOrder> newOrderQueue = newOrderStore.get(districtKey);
                NewOrder mostRecentOrder = newOrderQueue.peekLast();
                if (mostRecentOrder != null) {
                    assertEquals(expected, mostRecentOrder.orderId);
                }
            }
        }
    }
}
