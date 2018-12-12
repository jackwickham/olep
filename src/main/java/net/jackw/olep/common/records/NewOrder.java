package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public class NewOrder {
    public final int orderId;
    public final int warehouseId;
    public final int districtId;
    public final int customerId;
    public final BigDecimal totalAmount;

    public NewOrder(
        @JsonProperty("orderId") int orderId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("totalAmount") BigDecimal totalAmount
    ) {
        this.orderId = orderId;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
    }
}
