package net.jackw.olep.message.transaction_result;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import java.util.Map;

@Immutable
public class DeliveryResult extends TransactionResultMessage {
    public static class Builder extends PartialResult implements TransactionResultBuilder<DeliveryResult> {
        private final int warehouseId;
        private final int carrierId;

        public Builder(int warehouseId, int carrierId) {
            this.warehouseId = warehouseId;
            this.carrierId = carrierId;
        }

        @Override
        public boolean canBuild() {
            return processedOrders != null;
        }

        @Override
        public DeliveryResult build() {
            return new DeliveryResult(warehouseId, carrierId, ImmutableMap.copyOf(processedOrders));
        }
    }

    public static class PartialResult implements PartialTransactionResult {
        // All of the orders will be processed by one machine, so there's no need to make it mutable or handle merges
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
