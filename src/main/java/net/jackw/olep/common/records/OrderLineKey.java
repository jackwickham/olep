package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.Objects;

public class OrderLineKey {
    public final int orderId;
    public final int districtId;
    public final int warehouseId;
    public final int lineNumber;

    public OrderLineKey(
        @JsonProperty("orderId") int orderId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("lineNumber") int lineNumber
    ) {
        this.orderId = orderId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.lineNumber = lineNumber;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof OrderLine) {
            OrderLine other = (OrderLine) obj;
            return orderId == other.orderId && districtId == other.districtId && warehouseId == other.warehouseId &&
                lineNumber == other.lineNumber;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, districtId, warehouseId, lineNumber);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("orderId", orderId)
            .add("districtId", districtId)
            .add("warehouseId", warehouseId)
            .add("lineNumber", lineNumber)
            .toString();
    }
}
