package net.jackw.olep.common.records;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

public class Warehouse extends WarehouseShared {
    public final BigDecimal ytd;

    public Warehouse(int id, @Nonnull String name, @Nonnull Address address, @Nonnull BigDecimal tax, @Nonnull BigDecimal ytd) {
        super(id, name, address, tax);
        this.ytd = ytd;
    }
}
