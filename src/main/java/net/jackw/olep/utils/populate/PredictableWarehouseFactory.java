package net.jackw.olep.utils.populate;

import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.utils.CommonFieldGenerators;

import java.math.BigDecimal;

public class PredictableWarehouseFactory implements WarehouseFactory {
    private int nextId;

    private static PredictableWarehouseFactory instance = null;

    private PredictableWarehouseFactory() {
        nextId = 1;
    }

    public static PredictableWarehouseFactory getInstance() {
        if (instance == null) {
            instance = new PredictableWarehouseFactory();
        }
        return instance;
    }

    /**
     * Make a new warehouse, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    @Override
    public WarehouseShared makeWarehouseShared() {
        return getWarehouseShared(nextId++);
    }

    public WarehouseShared getWarehouseShared(int id) {
        String name = String.format("WHOUSE%d", id);
        Address address = CommonFieldGenerators.generatePredictableAddress("WAREHOUSE", id);
        BigDecimal tax = new BigDecimal(id % 200).movePointLeft(3);

        return new WarehouseShared(id, name, address, tax);
    }

    public static void resetInstance() {
        instance = null;
    }
}
