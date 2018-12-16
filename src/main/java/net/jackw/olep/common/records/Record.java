package net.jackw.olep.common.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.errorprone.annotations.ForOverride;
import com.google.errorprone.annotations.Immutable;

/**
 * An abstract database record
 *
 * @param <K> The type of a unique key for this record
 */
@Immutable
public abstract class Record<K> {
    @SuppressWarnings("Immutable")
    private K key = null;

    /**
     * Get a key that uniquely identifies this record
     *
     * This is usually the primary key as defined in TPC-C
     */
    @JsonIgnore
    public final K getKey() {
        if (key == null) {
            key = makeKey();
        }
        return key;
    }

    /**
     * Construct the key that uniquely identifies this record. The key will be cached, so it will only be called at most
     * once in the life of the object.
     *
     * This key is usually the key defined in TPC-C
     */
    @ForOverride
    protected abstract K makeKey();

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
