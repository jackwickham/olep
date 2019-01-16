package net.jackw.olep.view;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.OrderStatusResult;

import java.math.BigDecimal;

@Immutable
@CheckReturnValue
public class CustomerState {
    private final BigDecimal balance;
    private final int latestOrderId;
    private final long latestOrderDate;
    private final Integer latestOrderCarrierId;
    private final ImmutableList<? extends OrderLine> latestOrderLines;

    public CustomerState(BigDecimal balance, int latestOrderId, long latestOrderDate, Integer latestOrderCarrierId, ImmutableList<? extends OrderLine> latestOrderLines) {
        this.balance = balance;
        this.latestOrderId = latestOrderId;
        this.latestOrderDate = latestOrderDate;
        this.latestOrderCarrierId = latestOrderCarrierId;
        this.latestOrderLines = latestOrderLines;
    }

    public CustomerState withPayment(BigDecimal amount) {
        return new CustomerState(
            balance.subtract(amount), latestOrderId, latestOrderDate, latestOrderCarrierId, latestOrderLines
        );
    }

    public CustomerState withLatestOrder(int orderId, long orderDate, Integer carrierId, ImmutableList<? extends OrderLine> orderLines) {
        return new CustomerState(balance, orderId, orderDate, carrierId, orderLines);
    }

    public CustomerState withDelivery(int orderId, long deliveryDate, int carrierId, BigDecimal orderTotal) {
        ImmutableList<? extends OrderLine> lines;
        Integer resultingCarrierId;
        if (orderId == latestOrderId) {
            ImmutableList.Builder<OrderLine> lineBuilder = ImmutableList.builder();
            for (OrderLine line : latestOrderLines) {
                lineBuilder.add(line.withDeliveryDate(deliveryDate));
            }
            lines = lineBuilder.build();
            resultingCarrierId = carrierId;
        } else {
            // Not the latest order, so just update the customer's balance
            lines = latestOrderLines;
            resultingCarrierId = latestOrderCarrierId;
        }
        return new CustomerState(balance.add(orderTotal), latestOrderId, latestOrderDate, resultingCarrierId, lines);
    }

    public OrderStatusResult intoOrderStatusResult(CustomerShared shared) {
        return new OrderStatusResult(
            shared.id, shared.districtId, shared.warehouseId, shared.firstName, shared.middleName, shared.lastName,
            balance, latestOrderId, latestOrderDate, latestOrderCarrierId, latestOrderLines
        );
    }
}
