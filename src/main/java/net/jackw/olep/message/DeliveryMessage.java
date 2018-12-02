package net.jackw.olep.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class DeliveryMessage extends TransactionRequestBody {
    public final int warehouseId;
    public final int carrierId;
    public final long deliveryDate;

    public DeliveryMessage(
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("carrierId") int carrierId,
        @JsonProperty("deliveryDate") long deliveryDate
    ) {
        this.warehouseId = warehouseId;
        this.carrierId = carrierId;
        this.deliveryDate = deliveryDate;
    }
}
