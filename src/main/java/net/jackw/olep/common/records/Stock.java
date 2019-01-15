package net.jackw.olep.common.records;

import com.google.errorprone.annotations.Immutable;

@Immutable
public class Stock {
    public final StockShared stockShared;
    public final int stockQuantity;

    public Stock(StockShared stockShared, int stockQuantity) {
        this.stockShared = stockShared;
        this.stockQuantity = stockQuantity;
    }

    public WarehouseSpecificKey getKey() {
        return stockShared.getKey();
    }
}
