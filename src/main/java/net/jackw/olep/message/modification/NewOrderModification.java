package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.message.transaction_request.NewOrderRequest;

@Immutable
public class NewOrderModification implements ModificationMessage {
    public final int customerId;
    public final int districtId;
    public final int warehouseId;
    public final ImmutableList<OrderLineModification> lines;
    public final long date;
    public final int orderId;

    public NewOrderModification(
        @JsonProperty("customerId") int customerId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("lines") ImmutableList<OrderLineModification> lines,
        @JsonProperty("date") long date,
        @JsonProperty("orderId") int orderId
    ) {
        this.customerId = customerId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.lines = lines;
        this.date = date;
        this.orderId = orderId;
    }

    @Override
    @JsonIgnore
    public int getViewWarehouse() {
        return warehouseId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("customerId", customerId)
            .add("districtId", districtId)
            .add("warehouseId", warehouseId)
            .add("lines", lines)
            .add("date", date)
            .add("orderId", orderId)
            .toString();
    }
}
