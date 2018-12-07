package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

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
}
