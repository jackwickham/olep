package net.jackw.olep.edge.transaction_result;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.Credit;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Immutable
public class NewOrderResult extends TransactionResult {
    public static class Builder extends TransactionResultBuilder<NewOrderResult> {
        @Override
        public boolean canBuild() {
            return warehouseId != null && districtId != null && customerId != null && orderDate != null &&
                orderId != null && customerSurname != null && credit != null && discount != null &&
                warehouseTax != null && districtTax != null && lineCount != null && lines.size() == lineCount;
        }

        @Override
        public NewOrderResult build() {
            return new NewOrderResult(
                warehouseId, districtId, customerId, orderDate, orderId, customerSurname, credit, discount,
                warehouseTax, districtTax, ImmutableList.copyOf(lines)
            );
        }

        public Integer warehouseId;
        public Integer districtId;
        public Integer customerId;
        public Long orderDate;
        public Integer orderId;
        public String customerSurname;
        public Credit credit;
        public BigDecimal discount;
        public BigDecimal warehouseTax;
        public BigDecimal districtTax;
        @JsonMerge
        public List<OrderLineResult> lines = new ArrayList<>();
        public Integer lineCount;
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
    }
}
