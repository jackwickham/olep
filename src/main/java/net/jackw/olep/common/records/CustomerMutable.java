package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class CustomerMutable {
    public final BigDecimal balance;
    public final String data;

    public CustomerMutable(@JsonProperty("balance") BigDecimal balance, @JsonProperty("data") String data) {
        this.balance = balance;
        this.data = data;
    }
}
