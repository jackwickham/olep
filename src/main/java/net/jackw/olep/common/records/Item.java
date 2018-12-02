package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class Item {
    public final int id;
    public final int imId;
    public final String name;
    public final BigDecimal price;
    public final String data;

    public Item(@JsonProperty("id") int id, @JsonProperty("imId") int imId, @JsonProperty("name") String name, @JsonProperty("price") BigDecimal price, @JsonProperty("data") String data) {
        this.id = id;
        this.imId = imId;
        this.name = name;
        this.price = price;
        this.data = data;
    }
}
