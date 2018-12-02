package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class StockShared {
    public final int iId;
    public final int wId;
    public final String dist01;
    public final String dist02;
    public final String dist03;
    public final String dist04;
    public final String dist05;
    public final String dist06;
    public final String dist07;
    public final String dist08;
    public final String dist09;
    public final String dist10;
    public final String data;

    public StockShared(
        @JsonProperty("iId") int iId,
        @JsonProperty("wId") int wId,
        @JsonProperty("dist01") String dist01,
        @JsonProperty("dist02") String dist02,
        @JsonProperty("dist03") String dist03,
        @JsonProperty("dist04") String dist04,
        @JsonProperty("dist05") String dist05,
        @JsonProperty("dist06") String dist06,
        @JsonProperty("dist07") String dist07,
        @JsonProperty("dist08") String dist08,
        @JsonProperty("dist09") String dist09,
        @JsonProperty("dist10") String dist10,
        @JsonProperty("data") String data
    ) {
        this.iId = iId;
        this.wId = wId;
        this.dist01 = dist01;
        this.dist02 = dist02;
        this.dist03 = dist03;
        this.dist04 = dist04;
        this.dist05 = dist05;
        this.dist06 = dist06;
        this.dist07 = dist07;
        this.dist08 = dist08;
        this.dist09 = dist09;
        this.dist10 = dist10;
        this.data = data;
    }

    @JsonIgnore
    public Key getKey() {
        return new Key(iId, wId);
    }

    public static class Key {
        public final int iId;
        public final int wId;

        public Key(@JsonProperty("iId") int iId, @JsonProperty("wId") int wId) {
            this.iId = iId;
            this.wId = wId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Key) {
                Key other = (Key) obj;
                return iId == other.iId && wId == other.wId;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(iId) ^ Integer.hashCode(wId);
        }
    }
}
