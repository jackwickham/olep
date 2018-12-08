package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

@Immutable
public class Order extends Record<Order.Key> {
    public final int orderId;
    public final int districtId;
    public final int warehouseId;
    public final int customerId;
    public final long entryDate;
    @Nullable public final Integer carrierId;
    @Nonnull public final ImmutableList<OrderLine> orderLines;
    public final boolean allLocal;

    public Order(
        @JsonProperty("orderId") int orderId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("entryDate") long entryDate,
        @JsonProperty("carrierId") @Nullable Integer carrierId,
        @JsonProperty("orderLines") @Nonnull ImmutableList<OrderLine> orderLines,
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
    public Key getKey() {
        return new Key(orderId, districtId, warehouseId);
    }

    public static class Key {
        public final int orderId;
        public final int districtId;
        public final int warehouseId;

        public Key(
            @JsonProperty("orderId") int orderId,
            @JsonProperty("districtId") int districtId,
            @JsonProperty("warehouseId") int warehouseId
        ) {
            this.orderId = orderId;
            this.districtId = districtId;
            this.warehouseId = warehouseId;
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
}
