package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

@Immutable
public class Item extends Record<Integer> {
    public final int id;
    public final int imageId;
    @Nonnull
    public final String name;
    @Nonnull
    public final BigDecimal price;
    @Nonnull
    public final String data;

    public Item(
        @JsonProperty("id") int id,
        @JsonProperty("imageId") int imageId,
        @JsonProperty("name") @Nonnull String name,
        @JsonProperty("price") @Nonnull BigDecimal price,
        @JsonProperty("data") @Nonnull String data
    ) {
        this.id = id;
        this.imageId = imageId;
        this.name = name;
        this.price = price;
        this.data = data;
    }

    @Override @JsonIgnore
    public Integer getKey() {
        return id;
    }
}
