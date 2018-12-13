package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.util.Set;

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

    /**
     * Get the warehouses belonging to the workers that need to see this transaction
     */
    @Override
    @JsonIgnore
    public Set<Integer> getWorkerWarehouses() {
        return Set.of(warehouseId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("warehouseId", warehouseId)
            .add("carrierId", carrierId)
            .add("deliveryDate", deliveryDate)
            .toString();
    }
}
