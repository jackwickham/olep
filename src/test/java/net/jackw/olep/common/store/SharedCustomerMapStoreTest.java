package net.jackw.olep.common.store;

import net.jackw.olep.common.StoreKeyMissingException;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.store.SharedCustomerMapStore;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class SharedCustomerMapStoreTest {
    private SharedCustomerMapStore store;

    private int nextCustomerId = 1;

    private CustomerShared makeCustomer(String firstName, String lastName) {
        return new CustomerShared(
            nextCustomerId++, 1, 1, firstName, "A", lastName, new Address("", "", "", "", ""),
            "", 1L, Credit.GC, BigDecimal.ONE, BigDecimal.ZERO
        );
    }

    @Before
    public void initializeStore() {
        store = new SharedCustomerMapStore(2);
    }

    @Test
    public void testInsertGetFlow() {
        CustomerShared c = makeCustomer("", "l");
        assertNull(store.put(c.getKey(), c));
        assertTrue(store.containsKey(c.getKey()));
        assertSame(c, store.get(c.getKey()));
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
    public void testInsertingAddsToNameStore() {
        CustomerShared c = makeCustomer("", "LAST");
        store.put(c.getKey(), c);
        assertSame(c, store.get(c.getNameKey()));
    }

    @Test
    public void testLookupsByNameReturnMiddleCustomerSortedByFirstName() {
        CustomerShared cA = makeCustomer("A", "LAST");
        CustomerShared cB = makeCustomer("B", "LAST");
        CustomerShared cC = makeCustomer("C", "LAST");
        store.put(cA.getKey(), cA);
        store.put(cB.getKey(), cB);
        store.put(cC.getKey(), cC);

        assertSame(cB, store.get(cA.getNameKey()));
    }

    @Test
    public void testLookupsByNameRoundsUpWhenEvenCustomers() {
        CustomerShared cA = makeCustomer("A", "LAST");
        CustomerShared cB = makeCustomer("B", "LAST");
        CustomerShared cC = makeCustomer("C", "LAST");
        CustomerShared cD = makeCustomer("D", "LAST");
        store.put(cA.getKey(), cA);
        store.put(cD.getKey(), cD);
        store.put(cB.getKey(), cB);
        store.put(cC.getKey(), cC);

        assertSame(cC, store.get(cA.getNameKey()));
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

    @Test
    public void testGetBlockingNameDoesntBlockIfAlreadyInStore() throws InterruptedException {
        CustomerShared c = makeCustomer("", "LAST");
        store.put(c.getKey(), c);

        Thread.currentThread().interrupt();
        // If the thread tries to block, it will throw an InterruptedException and clear interrupted
        assertSame(c, store.getBlocking(c.getNameKey()));
        assertTrue(Thread.interrupted());
    }

    @Test
    public void testGetBlockingReturnsResultIfItBecomesAvailable() throws InterruptedException {
        final CustomerShared c = makeCustomer("", "LAST");

        new Thread(() -> {
            try {
                Thread.sleep(70);
                store.put(c.getKey(), c);
            } catch (InterruptedException e) {
                // :(
            }
        }).start();

        assertSame(c, store.getBlocking(c.getNameKey()));
    }

    @Test(expected = StoreKeyMissingException.class)
    public void testGetBlockingThrowsExceptionIfNoResultAppears() throws InterruptedException {
        store.getBlocking(new CustomerNameKey("TEST", 1, 1), 2);
    }
}
