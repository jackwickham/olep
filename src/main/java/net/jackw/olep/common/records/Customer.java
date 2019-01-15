package net.jackw.olep.common.records;

import com.google.errorprone.annotations.Immutable;

@Immutable
public class Customer {
    public final CustomerShared customerShared;
    public final CustomerMutable customerMutable;

    public Customer(CustomerShared customerShared, CustomerMutable customerMutable) {
        this.customerShared = customerShared;
        this.customerMutable = customerMutable;
    }

    public DistrictSpecificKey getKey() {
        return customerShared.getKey();
    }
}
