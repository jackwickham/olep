package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.errorprone.annotations.Immutable;

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
    //@JsonSubTypes.Type(value = PaymentRequest.class, name = "payment"),
    //@JsonSubTypes.Type(value = DeliveryRequest.class, name = "delivery")
})
@Immutable
public interface ModificationMessage {}
