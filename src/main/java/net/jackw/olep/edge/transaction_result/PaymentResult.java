package net.jackw.olep.edge.transaction_result;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;

import java.math.BigDecimal;

@Immutable
public class PaymentResult extends TransactionResult {
    public static class Builder extends TransactionResultBuilder<PaymentResult> {
        @Override
        public boolean canBuild() {
            return warehouseId != null && warehouseAddress != null && districtId != null && districtAddress != null &&
                customerWarehouseId != null && customerDistrictId != null && customerId != null &&
                customerAddress != null && customerPhone != null && customerSince != null && customerCredit != null &&
                customerCreditLimit != null && customerDiscount != null && customerBalance != null &&
                customerData != null;
        }

        @Override
        public PaymentResult build() {
            return new PaymentResult(
                warehouseId, warehouseAddress, districtId, districtAddress, customerWarehouseId, customerDistrictId,
                customerId, customerAddress, customerPhone, customerSince, customerCredit, customerCreditLimit,
                customerDiscount, customerBalance, customerData
            );
        }

        public Integer warehouseId;
        public Address warehouseAddress;
        public Integer districtId;
        public Address districtAddress;
        public Integer customerWarehouseId;
        public Integer customerDistrictId;
        public Integer customerId;
        public Address customerAddress;
        public String customerPhone;
        public Integer customerSince;
        public Credit customerCredit;
        public BigDecimal customerCreditLimit;
        public BigDecimal customerDiscount;
        public BigDecimal customerBalance;
        public String customerData;
    }

    public final int warehouseId;
    public final Address warehouseAddress;
    public final int districtId;
    public final Address districtAddress;
    public final int customerWarehouseId;
    public final int customerDistrictId;
    public final int customerId;
    public final Address customerAddress;
    public final String customerPhone;
    public final long customerSince;
    public final Credit customerCredit;
    public final BigDecimal customerCreditLimit;
    public final BigDecimal customerDiscount;
    public final BigDecimal customerBalance;
    public final String customerData;

    public PaymentResult(
        int warehouseId,
        Address warehouseAddress,
        int districtId,
        Address districtAddress,
        int customerWarehouseId,
        int customerDistrictId,
        int customerId,
        Address customerAddress,
        String customerPhone,
        long customerSince,
        Credit customerCredit,
        BigDecimal customerCreditLimit,
        BigDecimal customerDiscount,
        BigDecimal customerBalance,
        String customerData
    ) {
        this.warehouseId = warehouseId;
        this.warehouseAddress = warehouseAddress;
        this.districtId = districtId;
        this.districtAddress = districtAddress;
        this.customerWarehouseId = customerWarehouseId;
        this.customerDistrictId = customerDistrictId;
        this.customerId = customerId;
        this.customerAddress = customerAddress;
        this.customerPhone = customerPhone;
        this.customerSince = customerSince;
        this.customerCredit = customerCredit;
        this.customerCreditLimit = customerCreditLimit;
        this.customerDiscount = customerDiscount;
        this.customerBalance = customerBalance;
        this.customerData = customerData;
    }
}
