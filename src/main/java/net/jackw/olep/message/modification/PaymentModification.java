package net.jackw.olep.message.modification;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;
import java.util.Objects;

@Immutable
public class PaymentModification implements ModificationMessage {
    public final int districtId;
    public final int warehouseId;
    public final int customerId;
    public final int customerDistrictId;
    public final int customerWarehouseId;
    public final BigDecimal amount;
    public final BigDecimal balance;
    public final String customerData;

    public PaymentModification(
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("customerDistrictId") int customerDistrictId,
        @JsonProperty("customerWarehouseId") int customerWarehouseId,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("balance") BigDecimal balance,
        @JsonProperty("customerData") String customerData
    ) {
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.customerId = customerId;
        this.customerDistrictId = customerDistrictId;
        this.customerWarehouseId = customerWarehouseId;
        this.amount = amount;
        this.balance = balance;
        this.customerData = customerData;
    }

    @Override
    @JsonIgnore
    public int getViewWarehouse() {
        return warehouseId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("districtId", districtId)
            .add("warehouseId", warehouseId)
            .add("customerId", customerId)
            .add("customerDistrictId", customerDistrictId)
            .add("customerWarehouseId", customerWarehouseId)
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
            districtId,
            warehouseId,
            customerId,
            customerDistrictId,
            customerWarehouseId,
            amount,
            balance,
            customerData
        );
    }
}
