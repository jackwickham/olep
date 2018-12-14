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

    @Test
    public void testGetBlockingDoesntBlockIfAlreadyInStore() throws InterruptedException {
        Object o = new Object();
        store.put(8, o);

        Thread.currentThread().interrupt();
        // If the thread tries to block, it will throw an InterruptedException and clear interrupted
        assertSame(o, store.getBlocking(8));
        assertTrue(Thread.interrupted());
    }

    @Test
    public void testGetBlockingReturnsResultIfItBecomesAvailable() throws InterruptedException {
        final Object o = new Object();

        new Thread(() -> {
            try {
                Thread.sleep(70);
                store.put(1, o);
            } catch (InterruptedException e) {
                // :(
            }
        }).start();

        assertSame(o, store.getBlocking(1));
    }

    @Test(expected = StoreKeyMissingException.class)
    public void testGetBlockingThrowsExceptionIfNoResultAppears() throws InterruptedException {
        store.getBlocking(9, 2);
    }
}
