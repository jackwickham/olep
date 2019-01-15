package net.jackw.olep.utils.populate;

import net.jackw.olep.common.records.Item;

import java.math.BigDecimal;

public class PredictableItemFactory implements ItemFactory {
    private int nextId;

    private static PredictableItemFactory instance = null;

    private PredictableItemFactory() {
        nextId = 1;
    }

    public static PredictableItemFactory getInstance() {
        if (instance == null) {
            instance = new PredictableItemFactory();
        }
        return instance;
    }

    /**
     * Make a new item, populating fields randomly per the TPC-C spec
     */
    @Override
    public Item makeItem() {
        return getItem(nextId++);
    }

    public Item getItem(int id) {
        int imId = (id % 10_000) + 1;
        String name = String.format("Item number %d", id);
        BigDecimal price = new BigDecimal(id).add(new BigDecimal(id % 100).movePointLeft(2));
        String data = String.format("Data for item number %d", id);

        return new Item(id, imId, name, price, data);
    }
}
