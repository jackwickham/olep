package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class DeliveryRequest extends TransactionRequestMessage {
    public final int warehouseId;
    public final int carrierId;
    public final long deliveryDate;

    public DeliveryRequest(
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("carrierId") int carrierId,
        @JsonProperty("deliveryDate") long deliveryDate
    ) {
        this.warehouseId = warehouseId;
        this.carrierId = carrierId;
        this.deliveryDate = deliveryDate;
    }
}
