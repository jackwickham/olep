package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;
import java.util.Objects;

@Immutable
public class PaymentModification implements ModificationMessage {
    public final int warehouseId;
    public final int districtId;
    public final int customerId;
    public final int customerWarehouseId;
    public final int customerDistrictId;
    public final BigDecimal amount;
    public final BigDecimal balance;
    public final String customerData;

    public PaymentModification(
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("customerWarehouseId") int customerWarehouseId,
        @JsonProperty("customerDistrictId") int customerDistrictId,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("balance") BigDecimal balance,
        @JsonProperty("customerData") String customerData
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.customerWarehouseId = customerWarehouseId;
        this.customerDistrictId = customerDistrictId;
        this.amount = amount;
        this.balance = balance;
        this.customerData = customerData;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("warehouseId", warehouseId)
            .add("districtId", districtId)
            .add("customerId", customerId)
            .add("customerWarehouseId", customerWarehouseId)
            .add("customerDistrictId", customerDistrictId)
            .add("amount", amount)
            .add("balance", balance)
            .add("customerData", customerData)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof PaymentModification) {
            PaymentModification other = (PaymentModification) o;
            return warehouseId == other.warehouseId &&
                districtId == other.districtId &&
                customerId == other.customerId &&
                customerWarehouseId == other.customerWarehouseId &&
                customerDistrictId == other.customerDistrictId &&
                amount.equals(other.amount) &&
                balance.equals(other.balance) &&
                customerData.equals(other.customerData);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            warehouseId,
            districtId,
            customerId,
            customerWarehouseId,
            customerDistrictId,
            amount,
            balance,
            customerData
        );
    }
}
