package net.jackw.olep.common.records;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

@Immutable
public class CustomerNameKey {
    public final int warehouseId;
    public final int districtId;
    public final String lastName;

    public CustomerNameKey(
        int warehouseId,
        int districtId,
        String lastName
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.lastName = lastName;
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
            .add("warehouseId", warehouseId)
            .add("districtId", districtId)
            .add("name", lastName)
            .toString();
    }
}
