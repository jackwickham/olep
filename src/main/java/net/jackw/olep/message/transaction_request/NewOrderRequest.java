package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Immutable
public class NewOrderRequest extends TransactionRequestMessage {
    public final int customerId;
    public final int warehouseId;
    public final int districtId;
    public final ImmutableList<OrderLine> lines;
    public final long date;

    public NewOrderRequest(
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
            @JsonProperty("quantity") int quantity
        ) {
            this.itemId = itemId;
            this.supplyingWarehouseId = supplyingWarehouseId;
            this.quantity = quantity;
        }
    }

    @Override
    @JsonIgnore
    public Set<Integer> getWorkerWarehouses() {
        Set<Integer> warehouses = lines.stream()
            .map(line -> line.supplyingWarehouseId)
            .collect(Collectors.toSet());
        warehouses.add(warehouseId);
        return warehouses;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("customerId", customerId)
            .add("warehouseId", warehouseId)
            .add("districtId", districtId)
            .add("lines", lines)
            .add("date", date)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NewOrderRequest)) return false;
        NewOrderRequest that = (NewOrderRequest) o;
        return customerId == that.customerId &&
            warehouseId == that.warehouseId &&
            districtId == that.districtId &&
            date == that.date &&
            lines.equals(that.lines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerId, warehouseId, districtId, lines, date);
    }
}
