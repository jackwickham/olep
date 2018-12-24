package net.jackw.olep.view.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.OrderLine;

import java.math.BigDecimal;

/**
 * A denormalised customer view
 */
@Immutable
public class Customer {
    public final int id;
    public final int districtId;
    public final int warehouseId;
    public final String firstName;
    public final String middleName;
    public final String lastName;
    public final BigDecimal balance;
    public final int latestOrderId;
    public final long latestOrderDate;
    public final Integer latestOrderCarrierId;
    public final ImmutableList<OrderLine> latestOrderLines;

    public Customer(
        @JsonProperty("id") int id,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("firstName") String firstName,
        @JsonProperty("middleName") String middleName,
        @JsonProperty("lastName") String lastName,
        @JsonProperty("balance") BigDecimal balance,
        @JsonProperty("latestOrderId") int latestOrderId,
        @JsonProperty("latestOrderDate") long latestOrderDate,
        @JsonProperty("latestOrderCarrierId") Integer latestOrderCarrierId,
        @JsonProperty("latestOrderLines") ImmutableList<OrderLine> latestOrderLines
    ) {
        this.id = id;
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

    /**
     * Create a copy of this customer with details of a different order
     */
    public Customer withOrder(int orderId, long orderDate, Integer orderCarrierId, ImmutableList<OrderLine> orderLines) {
        return new Customer(
            id, districtId, warehouseId, firstName, middleName, lastName, balance, orderId, orderDate, orderCarrierId,
            orderLines
        );
    }

    /**
     * Create a copy of this customer after one of their orders has been delivered
     */
    public Customer afterDelivery(int orderId, long deliveryDate, int carrierId, BigDecimal orderTotal) {
        if (orderId != latestOrderId) {
            return this;
        }

        ImmutableList<OrderLine> updatedLines = ImmutableList.copyOf(
            Lists.transform(latestOrderLines, line -> line.withDeliveryDate(deliveryDate))
        );
        return new Customer(
            id, districtId, warehouseId, firstName, middleName, lastName, balance.add(orderTotal), latestOrderId,
            latestOrderDate, carrierId, updatedLines
        );
    }

    public Customer withBalance(BigDecimal balance) {
        return new Customer(
            id, districtId, warehouseId, firstName, middleName, lastName, balance, latestOrderId, latestOrderDate,
            latestOrderCarrierId, latestOrderLines
        );
    }
}
