package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

/**
 * A single delivery's modification log entry
 */
@Immutable
public class DeliveryModification implements ModificationMessage {
    public final int warehouseId;
    public final int districtId;
    public final int orderId;
    public final int carrierId;
    public final int customerId;
    public final BigDecimal orderTotal;

    public DeliveryModification(
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("orderId") int orderId,
        @JsonProperty("carrierId") int carrierId,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("orderTotal") BigDecimal orderTotal
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.orderId = orderId;
        this.carrierId = carrierId;
        this.customerId = customerId;
        this.orderTotal = orderTotal;
    }
}
