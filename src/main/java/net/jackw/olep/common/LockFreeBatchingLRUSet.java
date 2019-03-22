package net.jackw.olep.common;

import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A weaker LRU Set
 *
 * This set makes a best effort to
 */
public class LockFreeBatchingLRUSet<T> implements LRUSet<T> {
    private final int capacity;
    private final AtomicReference<ConcurrentHashMap<T, Object>> current;
    private final AtomicReference<ConcurrentHashMap<T, Object>> previous;
    private final AtomicInteger currentSize = new AtomicInteger(0);
    private final AtomicInteger generation = new AtomicInteger(1);

    private static final Object PRESENT = new Object();

    public LockFreeBatchingLRUSet(int capacity) {
        Preconditions.checkArgument(capacity > 0, "Capacity must be greater than zero");
        this.capacity = capacity;
        current = new AtomicReference<>(new ConcurrentHashMap<>(capacity + 5));
        previous = new AtomicReference<>(new ConcurrentHashMap<>(0));
    }

    @Override
    public boolean add(T element) {
        int initialGeneration;
        // Choose a pair of maps that exist at a consistent point in time
        ConcurrentHashMap<T, Object> currentMap;
        ConcurrentHashMap<T, Object> previousMap;
        do {
            // Using getAcquire so that subsequent accesses aren't reordered before this
            initialGeneration = generation.getAcquire();
            // Using getOpaque for the loads because it doesn't matter which order these loads occur, just that they are
            // between the loads of generation
            currentMap = current.getOpaque();
            previousMap = previous.getOpaque();
        } while (initialGeneration == 0 || initialGeneration != generation.get());

        // Try to insert into the chosen current map
        if (currentMap.putIfAbsent(element, PRESENT) != null) {
            // The element was already present in current, so there's nothing more we need to do
            return false;
        }
        // We performed an insertion, so increment the number of items in current
        // This may occur after another thread has zeroed out currentSize, but that doesn't matter because we don't
        // provide any guarantees that the size is exactly accurate, just that it roughly corresponds to the actual size
        int newSize = currentSize.incrementAndGet();

        // Check whether we need to swap current and previous
        if (newSize == capacity) {
            swapSets();
        }

        // Finally, check whether we did actually insert a new element, or whether it actually existed in previousMap
        return !previousMap.containsKey(element);
    }

    @Override
    public boolean contains(T element) {
        int initialGeneration;
        boolean result;
        do {
            // Get the generation at the start, using getAcquire so that subsequent accesses aren't reordered before this
            initialGeneration = generation.getAcquire();
            // See whether the value is in the current or previous set
            // Using getOpaque because the only constraint is that these reads are not reordered with respect to the
            // loads of the generation
            result = current.getOpaque().containsKey(element) || previous.getOpaque().containsKey(element);
            // Make sure we didn't swap during the loop
            // If the write lock was held the whole time (generation == 0), it wouldn't be a problem, but we can't tell
            // whether the lock was actually released and subsequently reacquired
        } while (initialGeneration == 0 || initialGeneration != generation.get());
        return result;
    }

    private void swapSets() {
        ConcurrentHashMap<T, Object> newMap = new ConcurrentHashMap<>(capacity + 5);
        // Acquire a write lock on the sets by setting the generation to zero
        // We only really need acquire semantics here, but AtomicInteger doesn't expose getAndSetAcquire
        int initialGeneration;
        do {
            initialGeneration = generation.getAndSet(0);
            // Wait for nobody else to be swapping them
        } while (initialGeneration == 0);

        // Now that we have a lock, move current to previous and set current to the new map
        previous.set(current.get());
        current.set(newMap);
        // And reset the current size
        currentSize.set(0);

        // Some writes may still be made to the old current map at this point, and they might increase currentSize, but
        // that's ok - they will still affect .contains
        //
        // In a particularly pathological case, if a write is delayed within add for so long that two swaps occur before
        // the write occurs, this will not be linearisable. It would be possible to fix that, but it's not necessary
        // for how it will be used. However, that means that the guarantees provided by this implementation are much
        // weaker than for LockFreeLRUSet

        // All done, release the lock
        generation.setRelease(initialGeneration + 1);
    }
}
