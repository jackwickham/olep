package net.jackw.olep.common;

/**
 * An implementation of LRUSet does not guarantee that calling {@link #add(T)}, then subsequently calling
 * {@link #contains(Object)} on the same value, will return true. The implementation is free to remove elements from the
 * set arbitrarily, at any time.
 */
public interface LRUSet<T> {
    /**
     * Add the element to the set
     *
     * @param element The element to add
     * @return false if the element was already present in the set, true otherwise
     */
    boolean add(T element);

    /**
     * Check whether this set contains element
     *
     * @param element The element to check for the presence of
     * @return true if the element is present in the set, false otherwise
     */
    boolean contains(T element);
}
