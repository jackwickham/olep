package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Set;

@Immutable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PaymentRequest extends TransactionRequestMessage {
    public final Integer customerId;
    public final String customerLastName;
    public final int districtId;
    public final int warehouseId;
    public final int customerDistrictId;
    public final int customerWarehouseId;
    public final BigDecimal amount;

    public PaymentRequest(
        int customerId,
        int districtId,
        int warehouseId,
        int customerDistrictId,
        int customerWarehouseId,
        BigDecimal amount
    ) {
        this(customerId, null, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount);
    }

    public PaymentRequest(
        String customerLastName,
        int districtId,
        int warehouseId,
        int customerDistrictId,
        int customerWarehouseId,
        BigDecimal amount
    ) {
        this(null, customerLastName, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount);
    }

    public PaymentRequest(
        @JsonProperty("customerId") Integer customerId,
        @JsonProperty("customerLastName") String customerLastName,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("customerDistrictId") int customerDistrictId,
        @JsonProperty("customerWarehouseId") int customerWarehouseId,
        @JsonProperty("amount") BigDecimal amount
    ) {
        this.customerId = customerId;
        this.customerLastName = customerLastName;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.customerDistrictId = customerDistrictId;
        this.customerWarehouseId = customerWarehouseId;
        this.amount = amount;
    }

    @Override
    @JsonIgnore
    public Set<Integer> getWorkerWarehouses() {
        return Set.of(customerWarehouseId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("customerId", customerId)
            .add("customerLastName", customerLastName)
            .add("districtId", districtId)
            .add("warehouseId", warehouseId)
            .add("customerDistrictId", customerDistrictId)
            .add("customerWarehouseId", customerWarehouseId)
            .add("amount", amount)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PaymentRequest)) return false;
        PaymentRequest that = (PaymentRequest) o;
        return warehouseId == that.warehouseId &&
            districtId == that.districtId &&
            customerWarehouseId == that.customerWarehouseId &&
            customerDistrictId == that.customerDistrictId &&
            Objects.equals(customerId, that.customerId) &&
            Objects.equals(customerLastName, that.customerLastName) &&
            Objects.equals(amount, that.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            districtId,
            warehouseId,
            customerId,
            customerLastName,
            customerDistrictId,
            customerWarehouseId,
            amount
        );
    }
}
