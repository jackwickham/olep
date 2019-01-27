package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.io.Serializable;
import java.util.Objects;

@Immutable
public class WarehouseSpecificKey implements Serializable {
    public final int id;
    public final int warehouseId;

    public WarehouseSpecificKey(@JsonProperty("id") int id, @JsonProperty("warehouseId") int warehouseId) {
        this.id = id;
        this.warehouseId = warehouseId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof WarehouseSpecificKey) {
            WarehouseSpecificKey other = (WarehouseSpecificKey) obj;
            return id == other.id && warehouseId == other.warehouseId;
        }
        return false;
    }

    @Override
    public int hashCode() {
       return Objects.hash(id, warehouseId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("warehouseId", warehouseId)
            .toString();
    }
}
