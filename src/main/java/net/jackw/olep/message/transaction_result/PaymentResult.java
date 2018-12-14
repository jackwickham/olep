package net.jackw.olep.message.transaction_result;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;

import javax.annotation.Nullable;
import java.math.BigDecimal;

@Immutable
public class PaymentResult extends TransactionResultMessage {
    public static class Builder extends PartialResult implements TransactionResultBuilder<PaymentResult> {
        private final int warehouseId;
        private final int districtId;
        private final int customerWarehouseId;
        private final int customerDistrictId;

        public Builder(
            int warehouseId, int districtId, int customerWarehouseId, int customerDistrictId, int customerId
        ) {
            this(warehouseId, districtId, customerWarehouseId, customerDistrictId);
            this.customerId = customerId;
        }

        public Builder(int warehouseId, int districtId, int customerWarehouseId, int customerDistrictId) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.customerWarehouseId = customerWarehouseId;
            this.customerDistrictId = customerDistrictId;
        }

        @Override
        public boolean canBuild() {
            return warehouseAddress != null && districtAddress != null && customerId != null &&
                customerAddress != null && customerPhone != null && customerSince != null && customerCredit != null &&
                customerCreditLimit != null && customerDiscount != null && customerBalance != null;
            // Customer data is allowed to be null
        }

        @Override
        public PaymentResult build() {
            return new PaymentResult(
                warehouseId, warehouseAddress, districtId, districtAddress, customerWarehouseId, customerDistrictId,
                customerId, customerAddress, customerPhone, customerSince, customerCredit, customerCreditLimit,
                customerDiscount, customerBalance, customerData
            );
        }
    }

    public static class PartialResult implements PartialTransactionResult {
        public Address warehouseAddress;
        public Address districtAddress;
        public Integer customerId;
        public Address customerAddress;
        public String customerPhone;
        public Long customerSince;
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
    @Nullable public final String customerData;

    private PaymentResult(
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
        @Nullable String customerData
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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("warehouseId", warehouseId)
            .add("warehouseAddress", warehouseAddress)
            .add("districtId", districtId)
            .add("districtAddress", districtAddress)
            .add("customerWarehouseId", customerWarehouseId)
            .add("customerDistrictId", customerDistrictId)
            .add("customerId", customerId)
            .add("customerAddress", customerAddress)
            .add("customerPhone", customerPhone)
            .add("customerSince", customerSince)
            .add("customerCredit", customerCredit)
            .add("customerCreditLimit", customerCreditLimit)
            .add("customerDiscount", customerDiscount)
            .add("customerBalance", customerBalance)
            .add("customerData", customerData)
            .toString();
    }
}
