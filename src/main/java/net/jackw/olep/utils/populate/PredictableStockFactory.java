package net.jackw.olep.utils.populate;

import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;

import java.util.HashMap;
import java.util.Map;

public class PredictableStockFactory implements StockFactory {
    private int nextItemId;
    private int warehouseId;

    private static Map<Integer, PredictableStockFactory> instances = new HashMap<>();

    private PredictableStockFactory(int warehouseId) {
        nextItemId = 1;
        this.warehouseId = warehouseId;
    }

    public static PredictableStockFactory instanceFor(WarehouseShared warehouse) {
        return instanceFor(warehouse.id);
    }

    public static PredictableStockFactory instanceFor(int warehouseId) {
        if (!instances.containsKey(warehouseId)) {
            instances.put(warehouseId, new PredictableStockFactory(warehouseId));
        }
        return instances.get(warehouseId);
    }

    @Override
    public Stock makeStock() {
        int itemId = nextItemId++;
        return new Stock(getStockShared(itemId), getStockQuantity(itemId));
    }


    /**
     * Make a new stock record, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    public StockShared getStockShared(int id) {
        String[] distXX = new String[10];
        for (int i = 0; i < 10; i++) {
            distXX[i] = String.format("%024d", id);
        }
        // S_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be
        // held by 8 consecutive characters starting at a random position within S_DATA
        String data = String.format("Data for stock number %d", id);

        return new StockShared(id, warehouseId, distXX[0], distXX[1], distXX[2], distXX[3], distXX[4], distXX[5],
            distXX[6], distXX[7], distXX[8], distXX[9], data);
    }

    public int getStockQuantity(int id) {
        return (id % 90) + 10;
    }

    public static void resetInstances() {
        instances = new HashMap<>();
    }
}
