package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;

@Immutable
public class OrderLine {
    public final int lineNumber;
    public final int itemId;
    public final int supplyWarehouseId;
    @Nullable public final Long deliveryDate;
    public final int quantity;
    @Nonnull public final BigDecimal amount;
    @Nonnull public final String distInfo;

    public OrderLine(
        @JsonProperty("lineNumber") int lineNumber,
        @JsonProperty("itemId") int itemId,
        @JsonProperty("supplyWarehouseId") int supplyWarehouseId,
        @JsonProperty("deliveryDate") @Nullable Long deliveryDate,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("amount") @Nonnull BigDecimal amount,
        @JsonProperty("distInfo") @Nonnull String distInfo
    ) {
        this.lineNumber = lineNumber;
        this.itemId = itemId;
        this.supplyWarehouseId = supplyWarehouseId;
        this.deliveryDate = deliveryDate;
        this.quantity = quantity;
        this.amount = amount;
        this.distInfo = distInfo;
    }

    public OrderLine(
        int lineNumber, int itemId, int supplyWarehouseId, int quantity, @Nonnull BigDecimal amount,
        @Nonnull String distInfo
    ) {
        this(lineNumber, itemId, supplyWarehouseId, null, quantity, amount, distInfo);
    }
}
