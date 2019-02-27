package net.jackw.olep.utils.populate;

import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;

public interface OrderFactory {
    UndeliveredOrder makeUndeliveredOrder(int customerId, StockProvider stockProvider);
    DeliveredOrder makeDeliveredOrder(int customerId, StockProvider stockProvider);
    int getNextOrderId();

    public static class UndeliveredOrder {
        public final NewOrderModification newOrderModification;
        public final NewOrder newOrder;

        public UndeliveredOrder(NewOrderModification newOrderModification, NewOrder newOrder) {
            this.newOrderModification = newOrderModification;
            this.newOrder = newOrder;
        }
    }

    public static class DeliveredOrder {
        public final NewOrderModification newOrderModification;
        public final DeliveryModification deliveryModification;

        public DeliveredOrder(NewOrderModification newOrderModification, DeliveryModification deliveryModification) {
            this.newOrderModification = newOrderModification;
            this.deliveryModification = deliveryModification;
        }
    }

    @FunctionalInterface
    public static interface StockProvider {
        int getStock(int itemId);
    }
}
