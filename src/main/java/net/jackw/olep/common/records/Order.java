package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class Order {
    public final int orderId;
    public final int districtId;
    public final int warehouseId;
    public final int customerId;
    public final long entryDate;
    @Nullable
    public final Integer carrierId;
    @Nonnull
    public final List<OrderLine> orderLines;
    public final boolean allLocal;

    public Order(
        @JsonProperty("orderId") int orderId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("entryDate") long entryDate,
        @JsonProperty("carrierId") @Nullable Integer carrierId,
        @JsonProperty("orderLines") @Nonnull List<OrderLine> orderLines,
        @JsonProperty("allLocal") boolean allLocal
    ) {
        this.orderId = orderId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.customerId = customerId;
        this.entryDate = entryDate;
        this.carrierId = carrierId;
        this.orderLines = orderLines;
        this.allLocal = allLocal;
    }

    public Order(
        int orderId, int districtId, int warehouseId, int customerId, long entryDate,
        @Nonnull List<OrderLine> orderLines, boolean allLocal
    ) {
        this(orderId, districtId, warehouseId, customerId, entryDate, null, orderLines, allLocal);
    }

    /**
     * Create a copy of this order instance, but with the specified carrier ID
     *
     * @param carrierId The O_CARRIER_ID for this order
     * @return A copy of this order, with carrierId set
     */
    public Order withCarrierId(int carrierId) {
        return new Order(orderId, districtId, warehouseId, customerId, entryDate, carrierId, orderLines, allLocal);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Order) {
            Order other = (Order) obj;
            return orderId == other.orderId && districtId == other.districtId && warehouseId == other.warehouseId;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, districtId, warehouseId);
    }
}
