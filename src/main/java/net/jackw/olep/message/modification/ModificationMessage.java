package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.message.transaction_request.PaymentRequest;

/**
 * Marker interface for modification events
 *
 * Modification events are are written to the modification log, so the views can perform the corresponding changes.
 * There is one modification event per transaction type.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = NewOrderModification.class, name = "neworder"),
    @JsonSubTypes.Type(value = PaymentModification.class, name = "payment"),
    @JsonSubTypes.Type(value = DeliveryModification.class, name = "delivery"),
    @JsonSubTypes.Type(value = RemoteStockModification.class, name="remote-stock")
})
@Immutable
public interface ModificationMessage {
    int getViewWarehouse();
}
