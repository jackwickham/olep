package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.message.transaction_request.NewOrderRequest;

@Immutable
public class NewOrderModification implements ModificationMessage {
    public final int customerId;
    public final int warehouseId;
    public final int districtId;
    public final ImmutableList<OrderLineModification> lines;
    public final long date;
    public final int orderId;

    public NewOrderModification(
        @JsonProperty("customerId") int customerId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("lines") ImmutableList<OrderLineModification> lines,
        @JsonProperty("date") long date,
        @JsonProperty("orderId") int orderId
    ) {
        this.customerId = customerId;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.lines = lines;
        this.date = date;
        this.orderId = orderId;
    }
}
