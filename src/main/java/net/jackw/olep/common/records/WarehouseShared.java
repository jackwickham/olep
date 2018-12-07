package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class WarehouseShared {
    public final int id;
    public final String name;
    public final Address address;
    public final BigDecimal tax;
    // ytd is worker-local

    public WarehouseShared(
        @JsonProperty("id") int id,
        @JsonProperty("name") String name,
        @JsonProperty("address") Address address,
        @JsonProperty("tax") BigDecimal tax
    ) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.tax = tax;
    }
}
