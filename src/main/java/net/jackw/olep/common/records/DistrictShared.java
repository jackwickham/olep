package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class DistrictShared extends Record<WarehouseSpecificKey> {
    public final int id;
    public final int warehouseId;
    public final String name;
    public final Address address;
    public final BigDecimal tax;

    public DistrictShared(
        @JsonProperty("id") int id,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("name") String name,
        @JsonProperty("address") Address address,
        @JsonProperty("tax") BigDecimal tax
    ) {
        this.id = id;
        this.warehouseId = warehouseId;
        this.name = name;
        this.address = address;
        this.tax = tax;
    }

    @Override @JsonIgnore
    public WarehouseSpecificKey getKey() {
        return new WarehouseSpecificKey(id, warehouseId);
    }
}
