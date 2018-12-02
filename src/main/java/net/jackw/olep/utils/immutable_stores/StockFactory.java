package net.jackw.olep.utils.immutable_stores;

import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.utils.RandomDataGenerator;

import java.util.HashMap;
import java.util.Map;

public class StockFactory {
    private int nextItemId;
    private RandomDataGenerator rand;
    private int warehouseId;

    private static Map<Integer, StockFactory> instances = new HashMap<>();

    private StockFactory(WarehouseShared warehouse) {
        nextItemId = 1;
        rand = new RandomDataGenerator();
        warehouseId = warehouse.id;
    }

    public static StockFactory instanceFor(WarehouseShared warehouse) {
        if (!instances.containsKey(warehouse.id)) {
            instances.put(warehouse.id, new StockFactory(warehouse));
        }
        return instances.get(warehouse.id);
    }

    /**
     * Make a new stock record, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    public StockShared makeStockShared() {
        // S_I_ID unique within [100,000]
        int iId = nextItemId++;
        // S_W_ID = W_ID
        int wId = warehouseId;
        // S_DIST_xx random a-string of 24 letters
        String[] distXX = new String[10];
        for (int i = 0; i < 10; i++) {
            distXX[i] = rand.aString(24, 24);
        }
        // S_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be
        // held by 8 consecutive characters starting at a random position within S_DATA
        String data = rand.aString(26, 50);
        if (rand.choice(10)) {
            int startIndex = rand.nextInt(data.length() - 8);
            data = data.subSequence(0, startIndex) + "ORIGINAL" + data.subSequence(startIndex + 8, data.length());
        }

        return new StockShared(iId, wId, distXX[0], distXX[1], distXX[2], distXX[3], distXX[4], distXX[5], distXX[6],
            distXX[7], distXX[8], distXX[9], data);
    }
}
