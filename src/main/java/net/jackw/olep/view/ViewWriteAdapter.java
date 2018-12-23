package net.jackw.olep.view;

import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.PaymentModification;

import java.io.Closeable;

public interface ViewWriteAdapter extends Closeable {
    void newOrder(NewOrderModification modification);
    void delivery(DeliveryModification modification);
    void payment(PaymentModification modification);
}
