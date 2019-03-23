package net.jackw.olep.common;

import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A weaker LRU Set
 *
 * This implementation guarantees linearisability, and will generally perform better than LRUSet. However, formal
 * lock-freedom is *not* provided.
 *
 * Approximately half the elements are removed from the set each time it reaches 2x capacity.
 */
public class BatchingLRUSet<T> implements LRUSet<T> {
    private final int capacity;
    private final AtomicReference<ConcurrentHashMap<T, Object>> current;
    private final AtomicReference<ConcurrentHashMap<T, Object>> previous;
    private final AtomicInteger currentSize = new AtomicInteger(0);
    // An even generation means things are static, and an odd generation means a swap is occurring
    private final AtomicInteger generation = new AtomicInteger(0);
    // Number of threads currently within add
    private final AtomicInteger currentWriters = new AtomicInteger(0);

    private static final Object PRESENT = new Object();

    public BatchingLRUSet(int capacity) {
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
            if ((initialGeneration & 1) == 1) {
                // Currently swapping - retry
                continue;
            }

            // Increment the number of threads writing first
            currentWriters.incrementAndGet();

            // Using getOpaque for the loads because generation ensures that they
            currentMap = current.getOpaque();
            previousMap = previous.getOpaque();

            // Make sure the generation hasn't changed since we started
            if (initialGeneration != generation.get()) {
                // Reset the counter and retry
                currentWriters.decrementAndGet();
            } else {
                break;
            }
        } while (true);

        // Try to insert into the chosen current map
        if (currentMap.putIfAbsent(element, PRESENT) != null) {
            // The element was already present in current, so there's nothing more we need to do
            // This insertion is linearised at the point where we read currentMap
            currentWriters.decrementAndGet();
            return false;
        }

        // We've finished the update, so release the lock on currentMap
        currentWriters.decrementAndGet();

        // Increment the number of items in current
        // This may occur after another thread has zeroed out currentSize, but that doesn't matter because we don't
        // provide any guarantees that the size is exactly accurate, just that it roughly corresponds to the actual size
        int newSize = currentSize.incrementAndGet();

        // Check whether we need to swap current and previous
        if (newSize == capacity) {
            swapSets();
        }

        // This doesn't need to be protected by currentWriters because it is guaranteed to be read-only
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
            // Full volatile reads are required here, because current must be read before previous
            ConcurrentHashMap<T, Object> currentMap = current.get();
            ConcurrentHashMap<T, Object> previousMap = previous.get();
            // Check the maps to see whether either of them contain the element in question
            result = currentMap.containsKey(element) || previousMap.containsKey(element);
            // If currentMap == previousMap, a swap was being performed while we ran. That's fine though, because that
            // will be a full set, and we can be linearised immediately after the swap
            // This means that static -> swapping -> static is ok, but swapping -> static -> swapping is not
            // Therefore, we can tolerate up to the next static generation
        } while ((initialGeneration | 1) + 1 < generation.get());
        return result;
    }

    private void swapSets() {
        ConcurrentHashMap<T, Object> newMap = new ConcurrentHashMap<>(capacity + 5);

        // Only one thread can have incremented the size to capacity, so we must be the only thread here
        // Increment the generation to notify .add that we are swapping
        //
        // Note that this implementation does not provide formal lock-freedom because if this thread is suspended after
        // incrementing the generation, or if a currentWriter is suspended which blocks this thread from making
        // progress, no adds can proceed.
        generation.incrementAndGet();

        // Wait for readers to become zero
        // Using a spin lock because it shouldn't last for long
        while (currentWriters.getAcquire() > 0) {
            // spin
        }

        // Move current to previous and set current to the new map
        // Previous must be set before current, so full volatile writes are required
        previous.set(current.get());
        current.set(newMap);

        // Some writes may still be made to the old current map at this point, and they might increase currentSize, but
        // that's ok - they will still affect .contains

        // Increase the generation to notify other threads that we have performed a swap
        generation.incrementAndGet();

        // Reset the size after updating the generation to guarantee that only one thread will be within the critical
        // section above at any time
        currentSize.setRelease(0);
    }
}
