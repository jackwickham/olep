package net.jackw.olep.common.store;

import net.jackw.olep.common.StoreKeyMissingException;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.*;

public abstract class BaseMapStoreTest {
    protected abstract WritableKeyValueStore<Val, Val> getStore();

    @Test
    public void testInsertGetFlow() {
        Val k = new Val(5);
        Val v =  new Val(1);
        getStore().put(k, v);
        assertTrue(getStore().containsKey(k));
        assertEquals(v, getStore().get(k));
    }

    @Test
    public void testRemove() {
        Val k = new Val(6);
        Val v = new Val(2);
        getStore().put(k, v);
        getStore().remove(k);
        assertFalse(getStore().containsKey(k));
        assertNull(getStore().get(k));
    }

    @Test
    public void testDuplicateInsertOverwrites() {
        Val k = new Val(2);
        Val v1 = new Val(3);
        Val v2 = new Val(4);

        getStore().put(k, v1);
        getStore().put(k, v2);
        assertEquals(v2, getStore().get(k));
    }

    @Test
    public void testGetBlockingDoesntBlockIfAlreadyInStore() throws InterruptedException {
        Val k = new Val(8);
        Val v = new Val(5);
        getStore().put(k, v);

        Thread.currentThread().interrupt();
        // If the thread tries to block, it will throw an InterruptedException and clear interrupted
        assertEquals(v, getStore().getBlocking(k));
        assertTrue(Thread.interrupted());
    }

    @Test
    public void testGetBlockingReturnsResultIfItBecomesAvailable() throws InterruptedException {
        final Val k = new Val(1);
        final Val v = new Val(6);

        new Thread(() -> {
            try {
                Thread.sleep(70);
                getStore().put(k, v);
            } catch (InterruptedException e) {
                // :(
            }
        }).start();

        assertEquals(v, getStore().getBlocking(k));
    }

    @Test(expected = StoreKeyMissingException.class)
    public void testGetBlockingThrowsExceptionIfNoResultAppears() throws InterruptedException {
        getStore().getBlocking(new Val(9), 2);
    }

    public static class Val implements Serializable {
        private int x;

        public Val(int x) {
            this.x = x;
        }

        public int getX() {
            return x;
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
