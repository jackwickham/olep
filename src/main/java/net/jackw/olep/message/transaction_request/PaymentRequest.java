package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;
import java.util.Set;

@Immutable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PaymentRequest extends TransactionRequestMessage {
    public final int warehouseId;
    public final int districtId;
    public final Integer customerId;
    public final String customerSurname;
    public final int customerWarehouseId;
    public final int customerDistrictId;
    public final BigDecimal amount;

    public PaymentRequest(
        int warehouseId,
        int districtId,
        int customerId,
        int customerWarehouseId,
        int customerDistrictId,
        BigDecimal amount
    ) {
        this(warehouseId, districtId, customerId, null, customerWarehouseId, customerDistrictId, amount);
    }

    public PaymentRequest(
        int warehouseId,
        int districtId,
        String customerSurname,
        int customerWarehouseId,
        int customerDistrictId,
        BigDecimal amount
    ) {
        this(warehouseId, districtId, null, customerSurname, customerWarehouseId, customerDistrictId, amount);
    }

    public PaymentRequest(
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("customerId") Integer customerId,
        @JsonProperty("customerSurname") String customerSurname,
        @JsonProperty("customerWarehouseId") int customerWarehouseId,
        @JsonProperty("customerDistrictId") int customerDistrictId,
        @JsonProperty("amount") BigDecimal amount
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.customerSurname = customerSurname;
        this.customerWarehouseId = customerWarehouseId;
        this.customerDistrictId = customerDistrictId;
        this.amount = amount;
    }

    @Override
    @JsonIgnore
    public Set<Integer> getWorkerWarehouses() {
        return Set.of(warehouseId);
    }
}
