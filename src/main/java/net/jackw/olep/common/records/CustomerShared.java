package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class CustomerShared {
    public final int id;
    public final int dId;
    public final int wId;
    public final String first;
    public final String middle;
    public final String last;
    public final Address address;
    public final String phone;
    // Java's Date class isn't immutable, which is bad, so just use long with the number of ms since 01 Jan 1970
    public final long since;
    public final Credit credit;
    public final BigDecimal creditLim;
    public final BigDecimal discount;

    public CustomerShared(
        @JsonProperty("id") int id,
        @JsonProperty("dId") int dId,
        @JsonProperty("wId") int wId,
        @JsonProperty("first") String first,
        @JsonProperty("middle") String middle,
        @JsonProperty("last") String last,
        @JsonProperty("address") Address address,
        @JsonProperty("phone") String phone,
        @JsonProperty("since") long since,
        @JsonProperty("credit") Credit credit,
        @JsonProperty("creditLim") BigDecimal creditLim,
        @JsonProperty("discount") BigDecimal discount
    ) {
        this.id = id;
        this.dId = dId;
        this.wId = wId;
        this.first = first;
        this.middle = middle;
        this.last = last;
        this.address = address;
        this.phone = phone;
        this.since = since;
        this.credit = credit;
        this.creditLim = creditLim;
        this.discount = discount;
    }

    @JsonIgnore
    public Key getKey() {
        return new Key(id, dId, wId);
    }

    public static class Key {
        public final int id;
        public final int dId;
        public final int wId;

        public Key(@JsonProperty("id") int id, @JsonProperty("dId") int dId, @JsonProperty("wId") int wId) {
            this.id = id;
            this.dId = dId;
            this.wId = wId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Key) {
                Key other = (Key) obj;
                return id == other.id && dId == other.dId && wId == other.wId;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(id) ^ Integer.hashCode(dId) ^ Integer.hashCode(wId);
        }
    }
}
