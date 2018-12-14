package net.jackw.olep.common;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SharedMapStoreTest {
    private SharedMapStore<Integer, Object> store;

    @Before
    public void initializeStore() {
        store = new SharedMapStore<>(2);
    }

    @Test
    public void testInsertGetFlow() {
        Object o =  new Object();
        assertNull(store.put(5, o));
        assertTrue(store.containsKey(5));
        assertSame(o, store.get(5));
    }

    @Test
    public void testRemove() {
        Object o = new Object();
        store.put(6, o);
        assertSame(o, store.remove(6));
        assertFalse(store.containsKey(6));
        assertNull(store.get(6));
    }

    @Test
    public void testDuplicateInsertOverwrites() {
        Object o1 = new Object();
        Object o2 = new Object();

        store.put(2, o1);
        assertSame(o1, store.put(2, o2));
        assertSame(o2, store.get(2));
    }
}
