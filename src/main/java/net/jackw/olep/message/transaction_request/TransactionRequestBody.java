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
    @JsonSubTypes.Type(value = TestMessage.class, name = "test"),
    @JsonSubTypes.Type(value = NewOrderMessage.class, name = "neworder")
})
@Immutable
public abstract class TransactionRequestBody {}
