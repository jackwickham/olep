package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;


public class NewOrder extends Record<DistrictSpecificKey> {
    public final int orderId;
    public final int districtId;
    public final int warehouseId;
    public final int customerId;
    public final BigDecimal totalAmount;

    public NewOrder(
        @JsonProperty("orderId") int orderId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("totalAmount") BigDecimal totalAmount
    ) {
        this.orderId = orderId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
    }

    @Override
    protected DistrictSpecificKey makeKey() {
        return new DistrictSpecificKey(orderId, districtId, warehouseId);
    }
}
