package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;

public class DistrictSpecificKey implements Serializable {
    public final int id;
    public final int districtId;
    public final int warehouseId;

    public DistrictSpecificKey(
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
        if (obj instanceof DistrictSpecificKey) {
            DistrictSpecificKey other = (DistrictSpecificKey) obj;
            return id == other.id && districtId == other.districtId && warehouseId == other.warehouseId;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, districtId, warehouseId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("districtId", districtId)
            .add("warehouseId", warehouseId)
            .toString();
    }
}
