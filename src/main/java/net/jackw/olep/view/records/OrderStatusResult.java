package net.jackw.olep.view.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.OrderLine;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * The result of an OrderStatus transaction
 */
@Immutable
public class OrderStatusResult implements Serializable {
    public final int customerId;
    public final int districtId;
    public final int warehouseId;
    public final String firstName;
    public final String middleName;
    public final String lastName;
    public final BigDecimal balance;
    public final int latestOrderId;
    public final long latestOrderDate;
    public final Integer latestOrderCarrierId;
    public final ImmutableList<? extends OrderLine> latestOrderLines;

    public OrderStatusResult(
        @JsonProperty("id") int customerId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("firstName") String firstName,
        @JsonProperty("middleName") String middleName,
        @JsonProperty("lastName") String lastName,
        @JsonProperty("balance") BigDecimal balance,
        @JsonProperty("latestOrderId") int latestOrderId,
        @JsonProperty("latestOrderDate") long latestOrderDate,
        @JsonProperty("latestOrderCarrierId") Integer latestOrderCarrierId,
        @JsonProperty("latestOrderLines") ImmutableList<? extends OrderLine> latestOrderLines
    ) {
        this.customerId = customerId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.firstName = firstName;
        this.middleName = middleName;
        this.lastName = lastName;
        this.balance = balance;
        this.latestOrderId = latestOrderId;
        this.latestOrderDate = latestOrderDate;
        this.latestOrderCarrierId = latestOrderCarrierId;
        this.latestOrderLines = latestOrderLines;
    }
}
