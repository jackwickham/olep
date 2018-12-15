package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

@Immutable
public class Address {
    public final String street1;
    public final String street2;
    public final String city;
    public final String state;
    public final String zip;

    public Address(
        @JsonProperty("street1") String street1,
        @JsonProperty("street2") String street2,
        @JsonProperty("city") String city,
        @JsonProperty("state") String state,
        @JsonProperty("zip") String zip
    ) {
        this.street1 = street1;
        this.street2 = street2;
        this.city = city;
        this.state = state;
        this.zip = zip;
    }

    @Override
    public int hashCode() {
        return Objects.hash(street1, street2, city, state, zip);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Address) {
            Address other = (Address) obj;
            return street1.equals(other.street1) && street2.equals(other.street2) && city.equals(other.city)
                && state.equals(other.state) && zip.equals(other.zip);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("street1", street1)
            .add("street2", street2)
            .add("city", city)
            .add("state", state)
            .add("zip", zip)
            .toString();
    }
}
