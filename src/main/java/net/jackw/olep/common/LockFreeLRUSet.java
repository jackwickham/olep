package net.jackw.olep.common;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Set that stores a maximum number of entries before discarding the oldest, using insertion order
 *
 * This set is thread-safe, and all calls to size, isEmpty, contains and add guarantee linearizability
 *
 * Linearizability is not provided for addAll.
 *
 * Removing elements from the set is not supported.
 *
 * @param <T> The contents of the set
 */
public class LockFreeLRUSet<T> extends AbstractSet<T> implements LRUSet<T>, Set<T> {
    // A map from element to successor key
    private final ConcurrentHashMap<T, AtomicReference<T>> map;
    // Pointer to the last element in the set
    private final AtomicReference<T> last = new AtomicReference<>(null);
    // Pointer to the first element in the set
    private final AtomicReference<T> first = new AtomicReference<>(null);
    // The maximum number of elements that this set is allowed to contain
    private final int capacity;
    // Approximately the number of elements that the set currently contains. This will never over-estimate the number of
    // elements, but it may under-estimate it if an element is currently being inserted or a deletion is being performed
    // that will subsequently fail
    private final AtomicInteger sizeInternal = new AtomicInteger();

    // Create a fake T (so we can reference it with an AtomicReference) to mark the node as being deleted
    // It's better to do the unsafe cast here than where it's used, or to have a map to Object, because this reduces
    // the number of unchecked casts, and therefore hopefully makes things safer
    @SuppressWarnings("unchecked")
    private final T BEING_DELETED = (T) new Object();

    /**
     * Create a new LRUSet with a maximum capacity
     *
     * @param capacity The maximum number of elements to hold in the set
     */
    public LockFreeLRUSet(int capacity) {
        Preconditions.checkArgument(capacity > 0, "Capacity of a LRUSet must be positive");

        this.capacity = capacity;
        this.map = new ConcurrentHashMap<>(capacity);
    }

    @Override
    public int size() {
        // Using map.size rather than sizeInternal because sizeInternal may erroneously return a value smaller than the
        // true size, which would break the linearizable guarantee
        return Math.min(map.size(), capacity);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    /**
     * Get an iterator for the elements in the set
     *
     * The iteration will be performed in an unspecified order (not insertion order)
     */
    @Override
    public Iterator<T> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public boolean add(@Nonnull T t) {
        Preconditions.checkNotNull(t, "Can't insert null into an LRUSet");

        AtomicReference<T> ref = new AtomicReference<>();
        if (map.putIfAbsent(t, ref) != null) {
            // Already in the map
            return false;
        } else {
            // We successfully inserted, so increment the size
            // This has to be done after the insertion because we can never over-estimate the number of elements, to
            // prevent too many elements from being deleted
            sizeInternal.incrementAndGet();

            // Get the current last element, then update the pointer to point to us
            T lastElem = last.getAndSet(t);

            // We are successfully inserted, now update the pointers
            if (lastElem == null) {
                // We're also the first, so update that pointer
                // The first pointer must still be null, because we just successfully inserted this element as the
                // successor of null, and null first pointers are ignored the rest of the time
                first.set(t);
            } else {
                // Update the next pointer of the predecessor to point to us (it must previously have been null)
                AtomicReference<T> oldLastSuccessor = map.get(lastElem);
                if (oldLastSuccessor == null || !oldLastSuccessor.compareAndSet(null, t)) {
                    // Because we replaced that element as the last, the CAS can only fail if that element is being
                    // deleted
                    // That means that we get to become the first element (spin until we are successful)
                    while (!first.compareAndSet(null, t)) { }
                }
            }

            checkSize();

            return true;
        }
    }

    /**
     * Check whether the set is over capacity, and remove an element if so
     */
    private void checkSize() {
        while(true) {
            T head = first.get();
            if (sizeInternal.get() > capacity) {
                // Decapitate it
                if (head != null && removeFirst(head)) {
                    break;
                } // If that failed, check again - that means there was a concurrent removal of the head
            } else {
                break;
            }
        }
    }

    /**
     * Remove the first element from the set, provided that it matches firstElem
     *
     * @param firstElem The element that should be removed. This element must be the first element in the set
     * @return Whether the removal was performed by this call
     */
    private boolean removeFirst(@Nonnull T firstElem) {
        // Get the successor, which will become the new first element
        AtomicReference<T> successorRef = map.get(firstElem);
        if (successorRef == null) {
            return false;
        }

        // Make sure nobody can add themselves as a successor
        T successor = successorRef.getAndSet(BEING_DELETED);
        if (successor == BEING_DELETED) {
            // Someone is already deleting this element
            return false;
        }

        // Decrease the size of the set now, to ensure that we never over-estimate the size of the set
        // If we end up failing, the size gets incremented again. If another thread checked the size while it was
        // decremented, it's not a problem because we are going to check it again in checkSize if it fails, and have
        // another go at deleting an element
        sizeInternal.decrementAndGet();

        // Point first to the next element
        if (!first.compareAndSet(firstElem, successor)) {
            // This element can't have been first
            // successorRef must still be BEING_DELETED, because we never allow that to be overwritten except here
            successorRef.set(successor);
            sizeInternal.incrementAndGet();
            return false;
        }

        // Now first has been successfully removed from the list, so also remove it from the set
        // The gap between removing it from the list and removing it from the set means that .contains may not be
        // consistent with .size, but otherwise shouldn't be a problem
        map.remove(firstElem);

        if (successor == null) {
            // We were the last element, so we also need to update that
            // If the CAS fails, something is being inserted as the last element, which means it is being added as a
            // successor of the element that was just deleted.
            // The inserter will notice that successor of this element is BEING_DELETED, and insert it as first instead
            last.compareAndSet(firstElem, null);
        }

        // The element was deleted
        return true;
    }

    @Override
    public boolean remove(Object o) {
        // This is very complex to implement safely
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        // This can't be (easily) implemented safely
        throw new UnsupportedOperationException();
    }
}
