package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import net.jackw.olep.message.transaction_request.NewOrderRequest;

public class NewOrderModification extends NewOrderRequest implements ModificationMessage {
    public final int orderId;

    public NewOrderModification(
        @JsonProperty("customerId") int customerId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("lines") ImmutableList<OrderLine> lines,
        @JsonProperty("date") long date,
        @JsonProperty("orderId") int orderId
    ) {
        super(customerId, warehouseId, districtId, lines, date);
        this.orderId = orderId;
    }

    public NewOrderModification(NewOrderRequest request, int orderId) {
        this(request.customerId, request.warehouseId, request.districtId, request.lines, request.date, orderId);
    }
}
