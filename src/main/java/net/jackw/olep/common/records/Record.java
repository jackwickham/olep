package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.errorprone.annotations.Immutable;

/**
 * An abstract database record
 *
 * @param <K> The type of a unique key for this record
 */
@Immutable
public abstract class Record<K> {
    /**
     * Get a key that uniquely identifies this record
     *
     * This is usually the primary key as defined in TPC-C
     */
    @JsonIgnore
    public abstract K getKey();

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Record && this.getClass().isInstance(obj)) {
            return getKey().equals(((Record)obj).getKey());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }
}
