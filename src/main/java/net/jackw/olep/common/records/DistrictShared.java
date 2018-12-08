package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;

@Immutable
public class DistrictShared extends Record<DistrictShared.Key> {
    public final int id;
    public final int wId;
    public final String name;
    public final Address address;
    public final BigDecimal tax;

    public DistrictShared(
        @JsonProperty("id") int id,
        @JsonProperty("warehouseId") int wId,
        @JsonProperty("name") String name,
        @JsonProperty("address") Address address,
        @JsonProperty("tax") BigDecimal tax
    ) {
        this.id = id;
        this.wId = wId;
        this.name = name;
        this.address = address;
        this.tax = tax;
    }

    @Override @JsonIgnore
    public Key getKey() {
        return new Key(id, wId);
    }

    @Immutable
    public static class Key {
        public final int id;
        public final int wId;

        public Key(@JsonProperty("id") int id, @JsonProperty("warehouseId") int wId) {
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
