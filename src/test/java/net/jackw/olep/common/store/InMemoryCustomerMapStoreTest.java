package net.jackw.olep.common.store;

import net.jackw.olep.common.records.CustomerShared;
import org.junit.Test;

import static org.junit.Assert.*;

public class InMemoryCustomerMapStoreTest extends BaseCustomerStoreTest {
    public InMemoryCustomerMapStoreTest() {
        super(new InMemoryCustomerMapStore(2));
    }

    @Test
    public void testRemoveFromIdStore() {
        CustomerShared c = makeCustomer("", "l");
        store.put(c.getKey(), c);
        assertSame(c, store.remove(c.getKey()));
        assertFalse(store.containsKey(c.getKey()));
        assertNull(store.get(c.getKey()));
    }

    @Test
    public void testRemoveAlsoRemovesFromNameStore() {
        CustomerShared c = makeCustomer("A", "LAST");
        store.put(c.getKey(), c);
        store.remove(c.getKey());

        assertNull(store.get(c.getNameKey()));
    }

    @Test
    public void testRemoveDoestRemoveOthersFromNameStore() {
        CustomerShared cA = makeCustomer("A", "LAST");
        CustomerShared cB = makeCustomer("B", "LAST");

        store.put(cA.getKey(), cA);
        store.put(cB.getKey(), cB);
        store.remove(cB.getKey());

        assertSame(cA, store.get(cB.getNameKey()));
    }
}
