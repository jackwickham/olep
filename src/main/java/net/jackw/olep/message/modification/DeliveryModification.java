package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;
import java.util.Objects;

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

    @Override
    public int hashCode() {
        return Objects.hash(warehouseId, districtId, orderId, carrierId, customerId, orderTotal);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DeliveryModification) {
            DeliveryModification other = (DeliveryModification) obj;
            return warehouseId == other.warehouseId && districtId == other.districtId && orderId == other.orderId &&
                carrierId == other.carrierId && customerId == other.customerId && orderTotal.equals(other.orderTotal);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("warehouseId", warehouseId)
            .add("districtId", districtId)
            .add("orderId", orderId)
            .add("carrierId", carrierId)
            .add("customerId", customerId)
            .add("orderTotal", orderId)
            .toString();
    }
}
