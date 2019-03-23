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
    // An even generation means things are static, and an odd generation means a swap is occurring
    private final AtomicInteger generation = new AtomicInteger(0);

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
            // Using getOpaque for the loads because generation ensures that they
            currentMap = current.getOpaque();
            previousMap = previous.getOpaque();
            // Make sure we're not currently swapping, and that the generation hasn't changed since
        } while ((initialGeneration & 1) == 1 || initialGeneration != generation.get());

        // If this thread is held up for so long here that two swaps occur, we still retain linearisability for `add`.
        // This is because if the same value was inserted by any other thread after 0 or 1 swaps, there will be a
        // collision on the following `put` call.
        //
        // If a collision occurs, this call can be linearised immediately after the conflicting `add`
        // If no collision occurs, no other insertions can have been performed in 0 or 1 swaps, so we can be linearised
        // somewhere in there.
        // This doesn't provide full linearisability with `contains` though.

        // Try to insert into the chosen current map
        if (currentMap.putIfAbsent(element, PRESENT) != null) {
            // The element was already present in current, so there's nothing more we need to do
            // This insertion is linearised at the point where we read currentMap
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

        // The following sequence of events is possible:
        //     T1 loads currentMap=m2 and previousMap=m1
        //     T2 performs a swap
        //     T3 loads currentMap=m3 and previousMap=m2
        //     T3 writes v to m3
        //     T3 checks m2 and discovers that v is not present, so returns true
        //     T1 writes v to m2
        //     T1 checks m1 and discovers that v is not present, so returns true
        // Only one of T1 and T3 should return successfully. Therefore, we need T3 to write v to m2, so that it gets
        // picked up by T1.
        return previousMap.put(element, PRESENT) == null;
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
        generation.incrementAndGet();

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
