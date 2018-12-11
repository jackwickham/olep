package net.jackw.olep.common.records;

import java.math.BigDecimal;

public class NewOrder {
    public final int warehouseId;
    public final int districtId;
    public final int customerId;
    public final BigDecimal totalAmount;

    public NewOrder(int warehouseId, int districtId, int customerId, BigDecimal totalAmount) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
    }
}
