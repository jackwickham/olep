package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Objects;

@Immutable
public class OrderLine extends Record<OrderLine.Key> {
    public final int orderId;
    public final int districtId;
    public final int warehouseId;
    public final int lineNumber;
    public final int itemId;
    public final int supplyWarehouseId;
    @Nullable public final Long deliveryDate;
    public final int quantity;
    @Nonnull public final BigDecimal amount;
    @Nonnull public final String distInfo;

    public OrderLine(
        @JsonProperty("orderId") int orderId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("lineNumber") int lineNumber,
        @JsonProperty("itemId") int itemId,
        @JsonProperty("supplyWarehouseId") int supplyWarehouseId,
        @JsonProperty("deliveryDate") @Nullable Long deliveryDate,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("amount") @Nonnull BigDecimal amount,
        @JsonProperty("distInfo") @Nonnull String distInfo
    ) {
        this.orderId = orderId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.lineNumber = lineNumber;
        this.itemId = itemId;
        this.supplyWarehouseId = supplyWarehouseId;
        this.deliveryDate = deliveryDate;
        this.quantity = quantity;
        this.amount = amount;
        this.distInfo = distInfo;
    }

    public OrderLine(
        int orderId, int districtId, int warehouseId, int lineNumber, int itemId, int supplyWarehouseId, int quantity,
        @Nonnull BigDecimal amount, @Nonnull String distInfo
    ) {
        this(
            orderId, districtId, warehouseId, lineNumber, itemId, supplyWarehouseId, null, quantity, amount,
            distInfo
        );
    }

    /**
     * Create a copy of this line, but with delivery date populated
     *
     * @param date The new delivery date
     * @return A copy of this OrderLine, with OL_DELIVERY_D populated
     */
    public OrderLine withDeliveryDate(long date) {
        return new OrderLine(
            orderId, districtId, warehouseId, lineNumber, itemId, supplyWarehouseId, date, quantity, amount, distInfo
        );
    }

    @Override
    public Key getKey() {
        return new Key(orderId, districtId, warehouseId, lineNumber);
    }

    public static class Key {
        public final int orderId;
        public final int districtId;
        public final int warehouseId;
        public final int lineNumber;

        public Key(
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
    }
}
