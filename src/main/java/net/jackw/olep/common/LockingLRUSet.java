package net.jackw.olep.common;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public class LockingLRUSet<T> implements Set<T> {
    private Set<T> set;
    private int capacity;

    public LockingLRUSet(int capacity) {
        Preconditions.checkArgument(capacity > 0, "Capacity of a LockingLRUSet must be positive");

        this.set = new LinkedHashSet<>(capacity);
        this.capacity = capacity;
    }

    @Override
    public synchronized int size() {
        return set.size();
    }

    @Override
    public synchronized boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public synchronized boolean contains(Object o) {
        return set.contains(o);
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return set.iterator();
    }

    @NotNull
    @Override
    public synchronized Object[] toArray() {
        return set.toArray();
    }

    @NotNull
    @Override
    public synchronized <T1> T1[] toArray(@NotNull T1[] a) {
        return set.toArray(a);
    }

    @Override
    public synchronized boolean add(T t) {
        boolean result = set.add(t);
        if (result && set.size() > capacity) {
            set.remove(set.iterator().next());
        }
        return result;
    }

    @Override
    public synchronized boolean remove(Object o) {
        return set.remove(o);
    }

    @Override
    public synchronized boolean containsAll(@NotNull Collection<?> c) {
        return set.containsAll(c);
    }

    @Override
    public synchronized boolean addAll(@NotNull Collection<? extends T> c) {
        boolean modified = false;
        for (T item : c){
            modified |= add(item);
        }
        return modified;
    }

    @Override
    public synchronized boolean retainAll(@NotNull Collection<?> c) {
        return set.retainAll(c);
    }

    @Override
    public synchronized boolean removeAll(@NotNull Collection<?> c) {
        return set.removeAll(c);
    }

    @Override
    public synchronized void clear() {
        set.clear();
    }
}
