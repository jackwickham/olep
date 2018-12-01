package net.jackw.olep.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class TestMessage extends TransactionRequestBody {
    public final String body;
    public final int item;

    public TestMessage(@JsonProperty("body") String body, @JsonProperty("item") int item) {
        this.body = body;
        this.item = item;
    }
}
