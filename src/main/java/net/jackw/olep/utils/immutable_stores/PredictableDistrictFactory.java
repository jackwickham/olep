package net.jackw.olep.utils.immutable_stores;

import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.utils.CommonFieldGenerators;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class PredictableDistrictFactory implements DistrictFactory {
    private int nextId;
    private int warehouseId;

    private static Map<Integer, PredictableDistrictFactory> instances = new HashMap<>();

    private PredictableDistrictFactory(int warehouseId) {
        nextId = 1;
        this.warehouseId = warehouseId;
    }

    public static PredictableDistrictFactory instanceFor(WarehouseShared warehouse) {
        return instanceFor(warehouse.id);
    }

    public static PredictableDistrictFactory instanceFor(int warehouseId) {
        if (!instances.containsKey(warehouseId)) {
            instances.put(warehouseId, new PredictableDistrictFactory(warehouseId));
        }
        return instances.get(warehouseId);
    }

    /**
     * Make a new district, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    @Override
    public DistrictShared makeDistrictShared() {
        return getDistrictShared(nextId++);
    }

    public DistrictShared getDistrictShared(int id) {
        int wId = warehouseId;
        String name = String.format("DISTR-%d", id);
        Address address = CommonFieldGenerators.generatePredictableAddress("DISTRICT", id);
        BigDecimal tax = new BigDecimal(id % 20).movePointLeft(2);
        return new DistrictShared(id, wId, name, address, tax);
    }
}
