package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Objects;

@Immutable
public class History {
    public final int customerId;
    public final int customerDistrictId;
    public final int customerWarehouseId;
    public final int districtId;
    public final int warehouseId;
    public final long date;
    @Nonnull public final BigDecimal amount;
    @Nonnull public final String data;

    public History(
        @JsonProperty("customerId") int customerId,
        @JsonProperty("customerDistrictId") int customerDistrictId,
        @JsonProperty("customerWarehouseId") int customerWarehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("date") long date,
        @Nonnull @JsonProperty("amount") BigDecimal amount,
        @Nonnull @JsonProperty("data") String data
    ) {
        this.customerId = customerId;
        this.customerDistrictId = customerDistrictId;
        this.customerWarehouseId = customerWarehouseId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.date = date;
        this.amount = amount;
        this.data = data;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof History) {
            History other = (History) obj;
            return customerId == other.customerId && customerDistrictId == other.customerDistrictId &&
                customerWarehouseId == other.customerWarehouseId && districtId == other.districtId &&
                warehouseId == other.warehouseId && date == other.date && amount.equals(other.amount) &&
                data.equals(other.data);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            customerId, customerDistrictId, customerWarehouseId, districtId, warehouseId, date, amount, data
        );
    }
}
