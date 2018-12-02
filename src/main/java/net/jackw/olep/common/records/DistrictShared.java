package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class DistrictShared {
    public final int id;
    public final int wId;
    public final String name;
    public final String street1;
    public final String street2;
    public final String city;
    public final String state;
    public final String zip;
    public final BigDecimal tax;

    public DistrictShared(
        @JsonProperty("id") int id,
        @JsonProperty("wId") int wId,
        @JsonProperty("name") String name,
        @JsonProperty("street1") String street1,
        @JsonProperty("street2") String street2,
        @JsonProperty("city") String city,
        @JsonProperty("state") String state,
        @JsonProperty("zip") String zip,
        @JsonProperty("tax") BigDecimal tax
    ) {
        this.id = id;
        this.wId = wId;
        this.name = name;
        this.street1 = street1;
        this.street2 = street2;
        this.city = city;
        this.state = state;
        this.zip = zip;
        this.tax = tax;
    }

    @JsonIgnore
    public Key getKey() {
        return new Key(id, wId);
    }

    @Immutable
    public static class Key {
        public final int id;
        public final int wId;

        public Key(@JsonProperty("id") int id, @JsonProperty("wId") int wId) {
            this.id = id;
            this.wId = wId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Key) {
                Key other = (Key) obj;
                return id == other.id && wId == other.wId;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(id) ^ Integer.hashCode(wId);
        }
    }
}
