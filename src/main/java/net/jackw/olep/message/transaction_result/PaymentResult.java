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
        private final int districtId;
        private final int warehouseId;
        private final int customerDistrictId;
        private final int customerWarehouseId;

        public Builder(
            int districtId, int warehouseId, int customerId, int customerDistrictId, int customerWarehouseId
        ) {
            this(districtId, warehouseId, customerDistrictId, customerWarehouseId);
            this.customerId = customerId;
        }

        public Builder(int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId) {
            this.districtId = districtId;
            this.warehouseId = warehouseId;
            this.customerDistrictId = customerDistrictId;
            this.customerWarehouseId = customerWarehouseId;
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
                districtId, districtAddress, warehouseId, warehouseAddress, customerId, customerDistrictId,
                customerWarehouseId, customerFirstName, customerMiddleName, customerLastName, customerAddress,
                customerPhone, customerSince, customerCredit, customerCreditLimit, customerDiscount, customerBalance,
                customerData
            );
        }
    }

    public static class PartialResult implements PartialTransactionResult {
        public Address districtAddress;
        public Address warehouseAddress;
        public Integer customerId;
        public String customerFirstName;
        public String customerMiddleName;
        public String customerLastName;
        public Address customerAddress;
        public String customerPhone;
        public Long customerSince;
        public Credit customerCredit;
        public BigDecimal customerCreditLimit;
        public BigDecimal customerDiscount;
        public BigDecimal customerBalance;
        public String customerData;
    }

    public final int districtId;
    public final Address districtAddress;
    public final int warehouseId;
    public final Address warehouseAddress;
    public final int customerId;
    public final int customerDistrictId;
    public final int customerWarehouseId;
    public final String customerFirstName;
    public final String customerMiddleName;
    public final String customerLastName;
    public final Address customerAddress;
    public final String customerPhone;
    public final long customerSince;
    public final Credit customerCredit;
    public final BigDecimal customerCreditLimit;
    public final BigDecimal customerDiscount;
    public final BigDecimal customerBalance;
    @Nullable public final String customerData;

    public PaymentResult(
        int districtId,
        Address districtAddress,
        int warehouseId,
        Address warehouseAddress,
        int customerId,
        int customerDistrictId,
        int customerWarehouseId,
        String customerFirstName,
        String customerMiddleName,
        String customerLastName,
        Address customerAddress,
        String customerPhone,
        long customerSince,
        Credit customerCredit,
        BigDecimal customerCreditLimit,
        BigDecimal customerDiscount,
        BigDecimal customerBalance,
        @Nullable String customerData
    ) {
        this.districtId = districtId;
        this.districtAddress = districtAddress;
        this.warehouseId = warehouseId;
        this.warehouseAddress = warehouseAddress;
        this.customerId = customerId;
        this.customerDistrictId = customerDistrictId;
        this.customerWarehouseId = customerWarehouseId;
        this.customerFirstName = customerFirstName;
        this.customerMiddleName = customerMiddleName;
        this.customerLastName = customerLastName;
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
            .add("districtId", districtId)
            .add("districtAddress", districtAddress)
            .add("warehouseId", warehouseId)
            .add("warehouseAddress", warehouseAddress)
            .add("customerId", customerId)
            .add("customerDistrictId", customerDistrictId)
            .add("customerWarehouseId", customerWarehouseId)
            .add("customerFirstName", customerFirstName)
            .add("customerMiddleName", customerMiddleName)
            .add("customerLastName", customerLastName)
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
