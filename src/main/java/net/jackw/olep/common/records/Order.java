package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public class Order extends Record<DistrictSpecificKey> {
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
    public DistrictSpecificKey getKey() {
        return new DistrictSpecificKey(orderId, districtId, warehouseId);
    }
}
