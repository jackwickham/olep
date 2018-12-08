package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Objects;

@Immutable
public class CustomerShared extends Record<CustomerShared.Key> {
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

    @Override @JsonIgnore
    public Key getKey() {
        return new Key(id, districtId, warehouseId);
    }

    public static class Key {
        public final int id;
        public final int districtId;
        public final int warehouseId;

        public Key(
            @JsonProperty("id") int id,
            @JsonProperty("districtId") int districtId,
            @JsonProperty("warehouseId") int warehouseId
        ) {
            this.id = id;
            this.districtId = districtId;
            this.warehouseId = warehouseId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Key) {
                Key other = (Key) obj;
                return id == other.id && districtId == other.districtId && warehouseId == other.warehouseId;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, districtId, warehouseId);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CustomerShared) {
            return getKey().equals(((CustomerShared)obj).getKey());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }
}
