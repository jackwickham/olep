package net.jackw.olep.utils.populate;

import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class RandomDistrictFactory implements DistrictFactory {
    private int nextId;
    private RandomDataGenerator rand;
    private int warehouseId;

    private static Map<Integer, RandomDistrictFactory> instances = new HashMap<>();

    private RandomDistrictFactory(WarehouseShared warehouse) {
        nextId = 1;
        rand = new RandomDataGenerator();
        warehouseId = warehouse.id;
    }

    public static RandomDistrictFactory instanceFor(WarehouseShared warehouse) {
        if (!instances.containsKey(warehouse.id)) {
            instances.put(warehouse.id, new RandomDistrictFactory(warehouse));
        }
        return instances.get(warehouse.id);
    }

    /**
     * Make a new district, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    @Override
    public DistrictShared makeDistrictShared() {
        // D_ID unique within [10]
        int id = nextId++;
        // D_W_ID = W_ID
        int wId = warehouseId;
        // D_NAME random a-string [6 .. 10]
        String name = rand.aString(6, 10);
        Address address = CommonFieldGenerators.generateAddress(rand);
        // D_TAX random within [0.0000 .. 0.2000]
        BigDecimal tax = rand.uniform(0L, 2000L, 4);

        return new DistrictShared(id, wId, name, address, tax);
    }

    public static void resetInstances() {
        instances = new HashMap<>();
    }
}
