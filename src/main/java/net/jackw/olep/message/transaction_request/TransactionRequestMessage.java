package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.errorprone.annotations.Immutable;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = NewOrderRequest.class, name = "neworder"),
    @JsonSubTypes.Type(value = PaymentRequest.class, name = "payment"),
    @JsonSubTypes.Type(value = DeliveryRequest.class, name = "delivery")
})
@Immutable
public abstract class TransactionRequestMessage {}
