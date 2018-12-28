package net.jackw.olep.utils.immutable_stores;

import net.jackw.olep.common.records.Address;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;
import net.jackw.olep.common.records.WarehouseShared;

import java.math.BigDecimal;

public class RandomWarehouseFactory implements WarehouseFactory {
    private int nextId;
    private RandomDataGenerator rand;

    private static RandomWarehouseFactory instance = null;

    private RandomWarehouseFactory() {
        nextId = 1;
        rand = new RandomDataGenerator();
    }

    public static RandomWarehouseFactory getInstance() {
        if (instance == null) {
            instance = new RandomWarehouseFactory();
        }
        return instance;
    }

    /**
     * Make a new warehouse, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    @Override
    public WarehouseShared makeWarehouseShared() {
        // W_ID unique within [number_of_configured_warehouses]
        int id = nextId++;
        // W_NAME random a-string [6 .. 10]
        String name = rand.aString(6, 10);
        Address address = CommonFieldGenerators.generateAddress(rand);
        // W_TAX random within [0.0000 .. 0.2000]
        BigDecimal tax = rand.uniform(0L, 2000L, 4);
        // W_YTD is not published to the shared store

        return new WarehouseShared(id, name, address, tax);
    }
}
