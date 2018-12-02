package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class WarehouseShared {
    public final int id;
    public final String name;
    public final String street1;
    public final String street2;
    public final String city;
    public final String state;
    public final String zip;
    public final BigDecimal tax;
    // ytd is worker-local

    public WarehouseShared(
        @JsonProperty("id") int id,
        @JsonProperty("name") String name,
        @JsonProperty("street1") String street1,
        @JsonProperty("street2") String street2,
        @JsonProperty("city") String city,
        @JsonProperty("state") String state,
        @JsonProperty("zip") String zip,
        @JsonProperty("tax") BigDecimal tax
    ) {
        this.id = id;
        this.name = name;
        this.street1 = street1;
        this.street2 = street2;
        this.city = city;
        this.state = state;
        this.zip = zip;
        this.tax = tax;
    }
}
