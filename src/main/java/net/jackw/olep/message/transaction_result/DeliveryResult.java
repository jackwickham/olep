package net.jackw.olep.message.transaction_result;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import java.util.Map;

@Immutable
public class DeliveryResult extends TransactionResult {
    public static class Builder extends TransactionResultBuilder<DeliveryResult> {
        @Override
        public boolean canBuild() {
            return warehouseId != null && carrierId != null && processedOrders != null;
        }

        @Override
        public DeliveryResult build() {
            return new DeliveryResult(warehouseId, carrierId, ImmutableMap.copyOf(processedOrders));
        }

        public Integer warehouseId;
        public Integer carrierId;
        public Map<Integer, Integer> processedOrders;
    }

    public final int warehouseId;
    public final int carrierId;
    public final ImmutableMap<Integer, Integer> processedOrders;

    public DeliveryResult(int warehouseId, int carrierId, ImmutableMap<Integer, Integer> processedOrders) {
        this.warehouseId = warehouseId;
        this.carrierId = carrierId;
        this.processedOrders = processedOrders;
    }
}
