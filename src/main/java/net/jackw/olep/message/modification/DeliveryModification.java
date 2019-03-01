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
    public final int orderId;
    public final int districtId;
    public final int warehouseId;
    public final int carrierId;
    public final long deliveryDate;
    public final int customerId;
    public final BigDecimal orderTotal;

    public DeliveryModification(
        @JsonProperty("orderId") int orderId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("carrierId") int carrierId,
        @JsonProperty("deliveryDate") long deliveryDate,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("orderTotal") BigDecimal orderTotal
    ) {
        this.orderId = orderId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.carrierId = carrierId;
        this.deliveryDate = deliveryDate;
        this.customerId = customerId;
        this.orderTotal = orderTotal;
    }

    @Override
    public int getViewWarehouse() {
        return warehouseId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, districtId, warehouseId, carrierId, deliveryDate, customerId, orderTotal);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DeliveryModification) {
            DeliveryModification other = (DeliveryModification) obj;
            return warehouseId == other.warehouseId && districtId == other.districtId && orderId == other.orderId &&
                carrierId == other.carrierId && customerId == other.customerId && orderTotal.equals(other.orderTotal) &&
                deliveryDate == other.deliveryDate;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("orderId", orderId)
            .add("districtId", districtId)
            .add("warehouseId", warehouseId)
            .add("carrierId", carrierId)
            .add("deliveryDate", deliveryDate)
            .add("customerId", customerId)
            .add("orderTotal", orderId)
            .toString();
    }
}
