package net.jackw.olep.message.transaction_result;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class OrderLineResult {
    public final int supplyWarehouseId;
    public final int itemId;
    public final String itemName;
    public final int quantity;
    public final int stockQuantity;
    public final BigDecimal itemPrice;
    public final BigDecimal lineAmount;

    public OrderLineResult(
        @JsonProperty("supplyWarehouseId") int supplyWarehouseId,
        @JsonProperty("itemId") int itemId,
        @JsonProperty("itemName") String itemName,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("stockQuantity") int stockQuantity,
        @JsonProperty("itemPrice") BigDecimal itemPrice,
        @JsonProperty("lineAmount") BigDecimal lineAmount
    ) {
        this.supplyWarehouseId = supplyWarehouseId;
        this.itemId = itemId;
        this.itemName = itemName;
        this.quantity = quantity;
        this.stockQuantity = stockQuantity;
        this.itemPrice = itemPrice;
        this.lineAmount = lineAmount;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("supplyWarehouseId", supplyWarehouseId)
            .add("itemId", itemId)
            .add("itemName", itemName)
            .add("quantity", quantity)
            .add("stockQuantity", stockQuantity)
            .add("itemPrice", itemPrice)
            .add("lineAmount", lineAmount)
            .toString();
    }

    public static class Builder extends PartialResult implements TransactionResultBuilder<OrderLineResult> {
        public final int supplyWarehouseId;
        public final int itemId;
        public final int quantity;

        public Builder(int supplyWarehouseId, int itemId, int quantity) {
            this.supplyWarehouseId = supplyWarehouseId;
            this.itemId = itemId;
            this.quantity = quantity;
        }

        @Override
        public boolean canBuild() {
            return itemName != null && stockQuantity != null && itemPrice != null && lineAmount != null;
        }

        @Override
        public OrderLineResult build() {
            return new OrderLineResult(
                supplyWarehouseId, itemId, itemName, quantity, stockQuantity, itemPrice, lineAmount
            );
        }
    }

    public static class PartialResult implements PartialTransactionResult {
        public String itemName;
        public Integer stockQuantity;
        public BigDecimal itemPrice;
        public BigDecimal lineAmount;

        public PartialResult() { }

        public PartialResult(String itemName, BigDecimal itemPrice, BigDecimal lineAmount) {
            this.itemName = itemName;
            this.itemPrice = itemPrice;
            this.lineAmount = lineAmount;
        }

        public PartialResult(int stockQuantity) {
            this.stockQuantity = stockQuantity;
        }

        public void mergeIn(PartialResult other) {
            if (other.itemName != null) {
                itemName = other.itemName;
            }
            if (other.stockQuantity != null) {
                stockQuantity = other.stockQuantity;
            }
            if (other.itemPrice != null) {
                itemPrice = other.itemPrice;
            }
            if (other.lineAmount != null) {
                lineAmount = other.lineAmount;
            }
        }
    }
}
