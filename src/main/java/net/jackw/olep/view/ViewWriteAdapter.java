package net.jackw.olep.view;

import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.modification.RemoteStockModification;

public interface ViewWriteAdapter extends AutoCloseable {
    void newOrder(NewOrderModification modification);
    void delivery(DeliveryModification modification);
    void payment(PaymentModification modification);
    void remoteStock(RemoteStockModification modification);

    /**
     * Register this adapter, so it can serve read transactions
     *
     * @return true if the registration succeeded, and false otherwise
     */
    boolean register(int partition);

    void unregister(int partition);

    @Override
    void close();
}
