package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.errorprone.annotations.Immutable;

import java.util.Set;

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
public abstract class TransactionRequestMessage {

    /**
     * Get the warehouses belonging to the workers that need to see this transaction
     */
    @JsonIgnore
    public abstract Set<Integer> getWorkerWarehouses();
}
