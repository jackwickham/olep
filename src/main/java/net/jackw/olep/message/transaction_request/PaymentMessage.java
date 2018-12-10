package net.jackw.olep.message.transaction_request;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PaymentMessage extends TransactionRequestBody {
    public final int warehouseId;
    public final int districtId;
    public final Integer customerId;
    public final String customerSurname;
    public final int customerWarehouseId;
    public final int customerDistrictId;
    public final BigDecimal amount;

    public PaymentMessage(
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("customerId") int customerId,
        @JsonProperty("customerWarehouseId") int customerWarehouseId,
        @JsonProperty("customerDistrictId") int customerDistrictId,
        @JsonProperty("amount") BigDecimal amount
    ) {
        this(warehouseId, districtId, customerId, null, customerWarehouseId, customerDistrictId, amount);
    }

    public PaymentMessage(
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("customerSurname") String customerSurname,
        @JsonProperty("customerWarehouseId") int customerWarehouseId,
        @JsonProperty("customerDistrictId") int customerDistrictId,
        @JsonProperty("amount") BigDecimal amount
    ) {
        this(warehouseId, districtId, null, customerSurname, customerWarehouseId, customerDistrictId, amount);
    }

    private PaymentMessage(
        int warehouseId, int districtId, Integer customerId, String customerSurname, int customerWarehouseId,
        int customerDistrictId, BigDecimal amount
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.customerSurname = customerSurname;
        this.customerWarehouseId = customerWarehouseId;
        this.customerDistrictId = customerDistrictId;
        this.amount = amount;
    }
}