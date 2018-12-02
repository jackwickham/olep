package net.jackw.olep.utils.immutable_stores;

import net.jackw.olep.utils.RandomDataGenerator;
import net.jackw.olep.common.records.WarehouseShared;

import java.math.BigDecimal;

public class WarehouseFactory {
    private int nextId;
    private RandomDataGenerator rand;

    private static WarehouseFactory instance = null;

    private WarehouseFactory() {
        nextId = 1;
        rand = new RandomDataGenerator();
    }

    public static WarehouseFactory getInstance() {
        if (instance == null) {
            instance = new WarehouseFactory();
        }
        return instance;
    }

    /**
     * Make a new warehouse, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    public WarehouseShared makeWarehouseShared() {
        // W_ID unique within [number_of_configured_warehouses]
        int id = nextId++;
        // W_NAME random a-string [6 .. 10]
        String name = rand.aString(6, 10);
        // W_STREET_1 random a-string [10 .. 20]
        String street1 = rand.aString(10, 20);
        // W_STREET_2 random a-string [10 .. 20]
        String street2 = rand.aString(10, 20);
        // W_CITY random a-string [10 .. 20]
        String city = rand.aString(10, 20);
        // W_STATE random a-string of 2 letters
        String state = rand.aString(2, 2);
        // W_ZIP is the concatenation of a random n-string of 4 numbers, and the constant string '11111'
        String zip = rand.nString(4, 4) + "11111";
        // W_TAX random within [0.0000 .. 0.2000]
        BigDecimal tax = rand.uniform(0L, 2000L, 4);
        // W_YTD is not published to the shared store

        return new WarehouseShared(id, name, street1, street2, city, state, zip, tax);
    }
}
