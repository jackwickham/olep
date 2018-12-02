package net.jackw.olep.utils.immutable_stores;

import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class DistrictFactory {
    private int nextId;
    private RandomDataGenerator rand;
    private int warehouseId;

    private static Map<Integer, DistrictFactory> instances = new HashMap<>();

    private DistrictFactory(WarehouseShared warehouse) {
        nextId = 1;
        rand = new RandomDataGenerator();
        warehouseId = warehouse.id;
    }

    public static DistrictFactory instanceFor(WarehouseShared warehouse) {
        if (!instances.containsKey(warehouse.id)) {
            instances.put(warehouse.id, new DistrictFactory(warehouse));
        }
        return instances.get(warehouse.id);
    }

    /**
     * Make a new district, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    public DistrictShared makeDistrictShared() {
        // D_ID unique within [10]
        int id = nextId++;
        // D_W_ID = W_ID
        int wId = warehouseId;
        // D_NAME random a-string [6 .. 10]
        String name = rand.aString(6, 10);
        // D_STREET_1 random a-string [10 .. 20]
        String street1 = rand.aString(10, 20);
        // D_STREET_2 random a-string [10 .. 20]
        String street2 = rand.aString(10, 20);
        // D_CITY random a-string [10 .. 20]
        String city = rand.aString(10, 20);
        // D_STATE random a-string of 2 letters
        String state = rand.aString(2, 2);
        // D_ZIP is the concatenation of a random n-string of 4 numbers, and the constant string '11111'
        String zip = rand.nString(4, 4) + "11111";
        // D_TAX random within [0.0000 .. 0.2000]
        BigDecimal tax = rand.uniform(0L, 2000L, 4);
        // D_YTD and D_NEXT_O_ID are not published to the shared store

        return new DistrictShared(id, wId, name, street1, street2, city, state, zip, tax);
    }
}
