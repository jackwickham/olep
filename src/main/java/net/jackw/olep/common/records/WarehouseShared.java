package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

@Immutable
public class WarehouseShared {
    public final int id;
    @Nonnull public final String name;
    @Nonnull public final Address address;
    @Nonnull public final BigDecimal tax;
    // ytd is worker-local

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
}
