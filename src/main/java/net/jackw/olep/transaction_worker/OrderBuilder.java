package net.jackw.olep.transaction_worker;

import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.records.Order;
import net.jackw.olep.common.records.OrderLine;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for an Order record
 */
public class OrderBuilder {
    private int orderId;
    private int districtId;
    private int warehouseId;
    private int customerId;
    private long entryDate;

    private List<OrderLine> orderLines;
    private Boolean allLocal = true;

    public OrderBuilder(int orderId, int districtId, int warehouseId, int customerId, long entryDate) {
        this.orderId = orderId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.customerId = customerId;
        this.entryDate = entryDate;

        this.orderLines = new ArrayList<>(15);
    }

    /**
     * Add a new OrderLine to this order's lines
     */
    public void addOrderLine(OrderLine line) {
        orderLines.add(line);
        if (line.warehouseId != warehouseId) {
            allLocal = false;
        }
    }

    /**
     * Convert this builder into an Order
     */
    public Order build() {
        return new Order(
            orderId, districtId, warehouseId, customerId, entryDate, null, ImmutableList.copyOf(orderLines),
            allLocal
        );
    }
}