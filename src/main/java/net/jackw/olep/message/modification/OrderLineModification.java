package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.OrderLine;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

@Immutable
public class OrderLineModification extends OrderLine {
    public final int homeWarehouseStockLevel;

    public OrderLineModification(
        @JsonProperty("lineNumber") int lineNumber,
        @JsonProperty("itemId") int itemId,
        @JsonProperty("supplyWarehouseId") int supplyWarehouseId,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("amount") @Nonnull BigDecimal amount,
        @JsonProperty("homeWarehouseStockLevel") int homeWarehouseStockLevel,
        @JsonProperty("distInfo") @Nonnull String distInfo
    ) {
        super(lineNumber, itemId, supplyWarehouseId, quantity, amount, distInfo);
        this.homeWarehouseStockLevel = homeWarehouseStockLevel;
    }
}
