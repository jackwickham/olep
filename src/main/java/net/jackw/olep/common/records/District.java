package net.jackw.olep.common.records;

import java.math.BigDecimal;

public class District extends DistrictShared {
    public final BigDecimal ytd;
    public final int nextOrderId;

    public District(int id, int warehouseId, String name, Address address, BigDecimal tax, BigDecimal ytd, int nextOrderId) {
        super(id, warehouseId, name, address, tax);
        this.ytd = ytd;
        this.nextOrderId = nextOrderId;
    }
}
