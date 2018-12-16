package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

@Immutable
public class WarehouseShared extends Record<Integer> {
    public final int id;
    @Nonnull public final String name;
    @Nonnull public final Address address;
    @Nonnull public final BigDecimal tax;

    public WarehouseShared(
        @JsonProperty("id") int id,
        @JsonProperty("name") @Nonnull String name,
        @JsonProperty("address") @Nonnull Address address,
        @JsonProperty("tax") @Nonnull BigDecimal tax
    ) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.tax = tax;
    }

    @Override @JsonIgnore
    public Integer getKey() {
        return id;
    }
}
