package net.jackw.olep.message.transaction_result;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.message.transaction_request.NewOrderRequest;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.SortedMap;
import java.util.TreeMap;

@Immutable
public class NewOrderResult extends TransactionResultMessage {
    public final int customerId;
    public final int districtId;
    public final int warehouseId;
    public final long orderDate;
    public final int orderId;
    public final String customerLastName;
    public final Credit credit;
    public final BigDecimal discount;
    public final BigDecimal warehouseTax;
    public final BigDecimal districtTax;
    public final ImmutableList<OrderLineResult> lines;

    private NewOrderResult(
        int customerId, int districtId, int warehouseId, long orderDate, int orderId, String customerLastName,
        Credit credit, BigDecimal discount, BigDecimal warehouseTax, BigDecimal districtTax,
        ImmutableList<OrderLineResult> lines
    ) {
        this.customerId = customerId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.orderDate = orderDate;
        this.orderId = orderId;
        this.customerLastName = customerLastName;
        this.credit = credit;
        this.discount = discount;
        this.warehouseTax = warehouseTax;
        this.districtTax = districtTax;
        this.lines = lines;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("customerId", this.customerId)
            .add("districtId", this.districtId)
            .add("warehouseId", this.warehouseId)
            .add("orderDate", new Date(this.orderDate))
            .add("orderId", this.orderId)
            .add("customerLastName", this.customerLastName)
            .add("credit", this.credit)
            .add("discount", this.discount)
            .add("warehouseTax", this.warehouseTax)
            .add("districtTax", this.districtTax)
            .add("lines", this.lines)
            .toString();
    }


    public static class Builder extends BasePartialResult implements TransactionResultBuilder<NewOrderResult> {
        private final int customerId;
        private final int districtId;
        private final int warehouseId;
        private final long orderDate;

        public Builder(
            int customerId, int districtId, int warehouseId, long orderDate, List<NewOrderRequest.OrderLine> lines
        ) {
            this.customerId = customerId;
            this.districtId = districtId;
            this.warehouseId = warehouseId;
            this.orderDate = orderDate;

            for (ListIterator<NewOrderRequest.OrderLine> it = lines.listIterator(); it.hasNext(); ) {
                NewOrderRequest.OrderLine line = it.next();
                OrderLineResult.Builder lineBuilder = new OrderLineResult.Builder(
                    line.supplyingWarehouseId, line.itemId, line.quantity
                );
                getLines().put(it.previousIndex(), lineBuilder);
            }
        }

        @Override
        public boolean canBuild() {
            return orderId != null && customerLastName != null && credit != null && discount != null &&
                warehouseTax != null && districtTax != null &&
                getLines().values().stream().allMatch(OrderLineResult.Builder::canBuild);
        }

        @Override
        public NewOrderResult build() {
            ImmutableList<OrderLineResult> lineResults = getLines().values().stream()
                .map(OrderLineResult.Builder::build)
                .collect(ImmutableList.toImmutableList());

            return new NewOrderResult(
                customerId, districtId, warehouseId, orderDate, orderId, customerLastName, credit, discount,
                warehouseTax, districtTax, lineResults
            );
        }

        private SortedMap<Integer, OrderLineResult.Builder> lines = new TreeMap<>();

        @JsonMerge
        @JsonProperty("lines")
        @Override
        public SortedMap<Integer, OrderLineResult.Builder> getLines() {
            return lines;
        }
    }

    public static class PartialResult extends BasePartialResult {
        private SortedMap<Integer, OrderLineResult.PartialResult> lines = new TreeMap<>();

        @JsonGetter("lines")
        @Override
        public SortedMap<Integer, OrderLineResult.PartialResult> getLines() {
            return lines;
        }

        public void addLine(int lineNumber, OrderLineResult.PartialResult line) {
            if (lines.containsKey(lineNumber)) {
                lines.get(lineNumber).mergeIn(line);
            } else {
                lines.put(lineNumber, line);
            }
        }
    }

    public static abstract class BasePartialResult implements PartialTransactionResult {
        public Integer orderId;
        public String customerLastName;
        public Credit credit;
        public BigDecimal discount;
        public BigDecimal warehouseTax;
        public BigDecimal districtTax;

        @JsonGetter("lines")
        public abstract SortedMap<Integer, ? extends OrderLineResult.PartialResult> getLines();
    }
}
