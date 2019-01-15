package net.jackw.olep.worker;

import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.OrderLineModification;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder for an Order modification
 */
public class NewOrderModificationBuilder {
    private int orderId;
    private int districtId;
    private int warehouseId;
    private int customerId;
    private long entryDate;

    private List<OrderLineModification> orderLines;
    private Boolean allLocal = true;

    private BigDecimal totalAmount = BigDecimal.ZERO;

    public NewOrderModificationBuilder(int orderId, int districtId, int warehouseId, int customerId, long entryDate) {
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
    public void addOrderLine(OrderLineModification line) {
        orderLines.add(line);
        if (line.supplyWarehouseId != warehouseId) {
            allLocal = false;
        }
        totalAmount = totalAmount.add(line.amount);
    }

    /**
     * Get the NewOrder associated with this order
     */
    public NewOrder buildNewOrder() {
        return new NewOrder(orderId, districtId, warehouseId, customerId, totalAmount);
    }

    /**
     * Get the order lines associated with this order
     */
    public ImmutableList<OrderLineModification> getLines() {
        return ImmutableList.copyOf(orderLines);
    }

    /**
     * Get the NewOrderModification that describes this order
     */
    public NewOrderModification build() {
        return new NewOrderModification(
            customerId, districtId, warehouseId, ImmutableList.copyOf(orderLines), entryDate, orderId
        );
    }
}
