package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.OrderLine;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

@Immutable
public class OrderLineModification {
    public final int lineNumber;
    public final int itemId;
    public final int supplyWarehouseId;
    public final int quantity;
    @Nonnull
    public final BigDecimal amount;
    public final int homeWarehouseStockLevel;
    @Nonnull
    public final String distInfo;

    public OrderLineModification(
        @JsonProperty("lineNumber") int lineNumber,
        @JsonProperty("itemId") int itemId,
        @JsonProperty("supplyWarehouseId") int supplyWarehouseId,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("amount") @Nonnull BigDecimal amount,
        @JsonProperty("homeWarehouseStockLevel") int homeWarehouseStockLevel,
        @JsonProperty("distInfo") @Nonnull String distInfo
    ) {
        this.lineNumber = lineNumber;
        this.itemId = itemId;
        this.supplyWarehouseId = supplyWarehouseId;
        this.quantity = quantity;
        this.amount = amount;
        this.homeWarehouseStockLevel = homeWarehouseStockLevel;
        this.distInfo = distInfo;
    }

    public OrderLine toOrderLine() {
        return new OrderLine(lineNumber, itemId, supplyWarehouseId, null, quantity, amount, distInfo);
    }
}
