package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class RemoteStockModification implements ModificationMessage {
    public final int itemId;
    public final int warehouseId;
    public final int stockLevel;

    public RemoteStockModification(
        @JsonProperty("itemId") int itemId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("stockLevel") int stockLevel
    ) {
        this.itemId = itemId;
        this.warehouseId = warehouseId;
        this.stockLevel = stockLevel;
    }

    @Override
    @JsonIgnore
    public int getViewWarehouse() {
        return warehouseId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("itemId", itemId)
            .add("warehouseId", warehouseId)
            .add("stockLevel", stockLevel)
            .toString();
    }
}
