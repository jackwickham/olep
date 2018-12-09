package net.jackw.olep.message.transaction_result;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.Credit;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Immutable
public class NewOrderResult extends TransactionResult {
    public static class Builder extends PartialResult implements TransactionResultBuilder<NewOrderResult> {
        private final int warehouseId;
        private final int districtId;
        private final int customerId;
        private final int lineCount;
        private final long orderDate;

        public Builder(int warehouseId, int districtId, int customerId, int lineCount, long orderDate) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.customerId = customerId;
            this.lineCount = lineCount;
            this.orderDate = orderDate;
        }

        @Override
        public boolean canBuild() {
            return orderId != null && customerSurname != null && credit != null && discount != null &&
                warehouseTax != null && districtTax != null && lines.size() == lineCount;
        }

        @Override
        public NewOrderResult build() {
            return new NewOrderResult(
                warehouseId, districtId, customerId, orderDate, orderId, customerSurname, credit, discount,
                warehouseTax, districtTax, ImmutableList.copyOf(lines)
            );
        }
    }

    public static class PartialResult implements PartialTransactionResult {
        public Integer orderId;
        public String customerSurname;
        public Credit credit;
        public BigDecimal discount;
        public BigDecimal warehouseTax;
        public BigDecimal districtTax;
        @JsonMerge
        public List<OrderLineResult> lines = new ArrayList<>();
    }

    public final int warehouseId;
    public final int districtId;
    public final int customerId;
    public final long orderDate;
    public final int orderId;
    public final String customerSurname;
    public final Credit credit;
    public final BigDecimal discount;
    public final BigDecimal warehouseTax;
    public final BigDecimal districtTax;
    public final ImmutableList<OrderLineResult> lines;

    private NewOrderResult(
        int warehouseId, int districtId, int customerId, long orderDate, int orderId, String customerSurname,
        Credit credit, BigDecimal discount, BigDecimal warehouseTax, BigDecimal districtTax,
        ImmutableList<OrderLineResult> lines
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.orderDate = orderDate;
        this.orderId = orderId;
        this.customerSurname = customerSurname;
        this.credit = credit;
        this.discount = discount;
        this.warehouseTax = warehouseTax;
        this.districtTax = districtTax;
        this.lines = lines;
    }

    @Immutable
    public static class OrderLineResult {
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
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("warehouseId", this.warehouseId)
            .add("districtId", this.districtId)
            .add("customerId", this.customerId)
            .add("orderDate", new Date(this.orderDate))
            .add("orderId", this.orderId)
            .add("customerSurname", this.customerSurname)
            .add("credit", this.credit)
            .add("discount", this.discount)
            .add("warehouseTax", this.warehouseTax)
            .add("districtTax", this.districtTax)
            .add("lines", this.lines)
            .toString();
    }
}
