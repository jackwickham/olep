package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class StockShared extends Record<WarehouseSpecificKey> {
    public final int itemId;
    public final int warehouseId;
    @Nonnull public final String dist01;
    @Nonnull public final String dist02;
    @Nonnull public final String dist03;
    @Nonnull public final String dist04;
    @Nonnull public final String dist05;
    @Nonnull public final String dist06;
    @Nonnull public final String dist07;
    @Nonnull public final String dist08;
    @Nonnull public final String dist09;
    @Nonnull public final String dist10;
    @Nonnull public final String data;

    public StockShared(
        @JsonProperty("itemId") int itemId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("dist01") @Nonnull String dist01,
        @JsonProperty("dist02") @Nonnull String dist02,
        @JsonProperty("dist03") @Nonnull String dist03,
        @JsonProperty("dist04") @Nonnull String dist04,
        @JsonProperty("dist05") @Nonnull String dist05,
        @JsonProperty("dist06") @Nonnull String dist06,
        @JsonProperty("dist07") @Nonnull String dist07,
        @JsonProperty("dist08") @Nonnull String dist08,
        @JsonProperty("dist09") @Nonnull String dist09,
        @JsonProperty("dist10") @Nonnull String dist10,
        @JsonProperty("data") @Nonnull String data
    ) {
        this.itemId = itemId;
        this.warehouseId = warehouseId;
        this.dist01 = dist01;
        this.dist02 = dist02;
        this.dist03 = dist03;
        this.dist04 = dist04;
        this.dist05 = dist05;
        this.dist06 = dist06;
        this.dist07 = dist07;
        this.dist08 = dist08;
        this.dist09 = dist09;
        this.dist10 = dist10;
        this.data = data;
    }

    /**
     * Get the S_DIST_XX field for this district
     *
     * @param district The district ID to use in the field key
     * @return The value of S_DIST_{district}
     */
    public String getDistrictInfo(int district) {
        switch (district) {
            case 1: return dist01;
            case 2: return dist02;
            case 3: return dist03;
            case 4: return dist04;
            case 5: return dist05;
            case 6: return dist06;
            case 7: return dist07;
            case 8: return dist08;
            case 9: return dist09;
            case 10: return dist10;
            default: throw new IllegalArgumentException("District must be between 1 and 10 inclusive");
        }
    }

    @Override @JsonIgnore
    public WarehouseSpecificKey getKey() {
        return new WarehouseSpecificKey(itemId, warehouseId);
    }
}
