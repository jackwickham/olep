package net.jackw.olep.common.records;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

@Immutable
public class CustomerNameKey {
    public final String lastName;
    public final int districtId;
    public final int warehouseId;

    public CustomerNameKey(
        String lastName,
        int districtId,
        int warehouseId
    ) {
        this.lastName = lastName;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CustomerNameKey) {
            CustomerNameKey other = (CustomerNameKey) obj;
            return lastName.equals(other.lastName) && districtId == other.districtId && warehouseId == other.warehouseId;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastName, districtId, warehouseId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", lastName)
            .add("districtId", districtId)
            .add("warehouseId", warehouseId)
            .toString();
    }
}
