package net.jackw.olep.utils.immutable_data;

import net.jackw.olep.application.data_generator.RandomDataGenerator;
import net.jackw.olep.common.records.Item;

import java.math.BigDecimal;

public class ItemFactory {
    private int nextId;
    private RandomDataGenerator rand;

    private static ItemFactory instance = null;

    private ItemFactory() {
        nextId = 1;
        rand = new RandomDataGenerator();
    }

    public static ItemFactory getInstance() {
        if (instance == null) {
            instance = new ItemFactory();
        }
        return instance;
    }

    /**
     * Make a new item, populating fields randomly per the TPC-C spec
     */
    public Item makeItem() {
        // I_ID unique within [100,000]
        int id = nextId++;
        // I_IM_ID random within [1 .. 10,000]
        int imId = rand.uniform(1, 10_000);
        // I_NAME random a-string [14 .. 24]
        String name = rand.aString(14, 24);
        // I_PRICE random within [1.00 .. 100.00]
        BigDecimal price = rand.uniform(1, 100, 2);
        // I_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string ORIGINAL must be held
        // by 8 consecutive characters starting at a random position within I_DATA
        String data = rand.aString(26, 50);
        if (rand.nextInt(10) == 0) {
            int startIndex = rand.nextInt(data.length() - 8);
            data = data.subSequence(0, startIndex) + "ORIGINAL" + data.subSequence(startIndex + 8, data.length());
        }

        return new Item(id, imId, name, price, data);
    }
}
