package net.jackw.olep.common.records;

import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class Item {
    public final int id;
    public final int imId;
    public final String name;
    public final BigDecimal price;
    public final String data;

    public Item(int id, int imId, String name, BigDecimal price, String data) {
        this.id = id;
        this.imId = imId;
        this.name = name;
        this.price = price;
        this.data = data;
    }
}
