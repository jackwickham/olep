package net.jackw.olep.common.store;

import net.jackw.olep.common.StoreKeyMissingException;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.*;

public abstract class BaseMapStoreTest {
    protected WritableKeyValueStore<Integer, Val> store;

    public BaseMapStoreTest(WritableKeyValueStore<Integer, Val> store) {
        this.store = store;
    }

    @Test
    public void testInsertGetFlow() {
        Val o =  new Val(1);
        assertNull(store.put(5, o));
        assertTrue(store.containsKey(5));
        assertEquals(o, store.get(5));
    }

    @Test
    public void testRemove() {
        Val o = new Val(2);
        store.put(6, o);
        assertEquals(o, store.remove(6));
        assertFalse(store.containsKey(6));
        assertNull(store.get(6));
    }

    @Test
    public void testDuplicateInsertOverwrites() {
        Val o1 = new Val(3);
        Val o2 = new Val(4);

        store.put(2, o1);
        assertEquals(o1, store.put(2, o2));
        assertEquals(o2, store.get(2));
    }

    @Test
    public void testGetBlockingDoesntBlockIfAlreadyInStore() throws InterruptedException {
        Val o = new Val(5);
        store.put(8, o);

        Thread.currentThread().interrupt();
        // If the thread tries to block, it will throw an InterruptedException and clear interrupted
        assertEquals(o, store.getBlocking(8));
        assertTrue(Thread.interrupted());
    }

    @Test
    public void testGetBlockingReturnsResultIfItBecomesAvailable() throws InterruptedException {
        final Val o = new Val(6);

        new Thread(() -> {
            try {
                Thread.sleep(70);
                store.put(1, o);
            } catch (InterruptedException e) {
                // :(
            }
        }).start();

        assertEquals(o, store.getBlocking(1));
    }

    @Test(expected = StoreKeyMissingException.class)
    public void testGetBlockingThrowsExceptionIfNoResultAppears() throws InterruptedException {
        store.getBlocking(9, 2);
    }

    public static class Val implements Serializable {
        private int x;

        public Val(int x) {
            this.x = x;
        }

        @Override
        public int hashCode() {
            return x;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Val) {
                return x == ((Val) obj).x;
            } else {
                return false;
            }
        }
    }
}
