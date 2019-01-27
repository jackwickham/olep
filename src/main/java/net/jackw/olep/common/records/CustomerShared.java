package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

@Immutable
public class CustomerShared extends Record<DistrictSpecificKey> {
    public final int id;
    public final int districtId;
    public final int warehouseId;
    @Nonnull
    public final String firstName;
    @Nonnull
    public final String middleName;
    @Nonnull
    public final String lastName;
    @Nonnull
    public final Address address;
    @Nonnull
    public final String phone;
    public final long since;
    @Nonnull
    public final Credit credit;
    @Nonnull
    public final BigDecimal creditLimit;
    @Nonnull
    public final BigDecimal discount;

    public CustomerShared(
        @JsonProperty("id") int id,
        @JsonProperty("districtId") int districtId,
        @JsonProperty("warehouseId") int warehouseId,
        @JsonProperty("firstName") @Nonnull String firstName,
        @JsonProperty("middleName") @Nonnull String middleName,
        @JsonProperty("lastName") @Nonnull String lastName,
        @JsonProperty("address") @Nonnull Address address,
        @JsonProperty("phone") @Nonnull String phone,
        @JsonProperty("since") long since,
        @JsonProperty("credit") @Nonnull Credit credit,
        @JsonProperty("creditLimit") @Nonnull BigDecimal creditLimit,
        @JsonProperty("discount") @Nonnull BigDecimal discount
    ) {
        this.id = id;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.firstName = firstName;
        this.middleName = middleName;
        this.lastName = lastName;
        this.address = address;
        this.phone = phone;
        this.since = since;
        this.credit = credit;
        this.creditLimit = creditLimit;
        this.discount = discount;
    }

    @Override
    protected DistrictSpecificKey makeKey() {
        return new DistrictSpecificKey(id, districtId, warehouseId);
    }

    @SuppressWarnings("Immutable")
    private CustomerNameKey nameKey = null;

    @JsonIgnore
    public CustomerNameKey getNameKey() {
        if (nameKey == null) {
            nameKey = new CustomerNameKey(lastName, districtId, warehouseId);
        }
        return nameKey;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("districtId", districtId)
            .add("warehouseId", warehouseId)
            .add("firstName", firstName)
            .add("middleName", middleName)
            .add("lastName", lastName)
            .add("address", address)
            .add("phone", phone)
            .add("since", since)
            .add("credit", credit)
            .add("creditLimit", creditLimit)
            .add("discount", discount)
            .toString();
    }
}
