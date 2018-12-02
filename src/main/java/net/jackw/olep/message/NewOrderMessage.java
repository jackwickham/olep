package net.jackw.olep.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class NewOrderMessage extends TransactionRequestBody {
    public final int customerId;
    public final int warehouseId;
    public final int districtId;
    public final ImmutableList<OrderLine> lines;
    public final long date;

    public NewOrderMessage(
        @JsonProperty("customerId") int customerId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("items") ImmutableList<OrderLine> lines,
        @JsonProperty("date") long date
    ) {
        this.customerId = customerId;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.lines = lines;
        this.date = date;
    }

    @Immutable
    public static class OrderLine {
        public final int itemId;
        public final int supplyingWarehouseId;
        public final int quantity;

        public OrderLine(
            @JsonProperty("itemId") int itemId,
            @JsonProperty("supplyingWarehouseId") int supplyingWarehouseId,
            @JsonProperty int quantity
        ) {
            this.itemId = itemId;
            this.supplyingWarehouseId = supplyingWarehouseId;
            this.quantity = quantity;
        }
    }
}
