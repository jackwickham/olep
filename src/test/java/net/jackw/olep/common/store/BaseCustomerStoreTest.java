package net.jackw.olep.common.store;

import com.google.common.base.MoreObjects;
import net.jackw.olep.common.StoreKeyMissingException;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.CustomerShared;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Objects;

import static org.junit.Assert.*;

public abstract class BaseCustomerStoreTest {
    protected WritableCustomerStore store;
    private int nextCustomerId = 1;

    public BaseCustomerStoreTest(WritableCustomerStore store) {
        this.store = store;
    }

    protected CustomerShared makeCustomer(String firstName, String lastName) {
        return new CustomerShared(
            nextCustomerId++, 1, 1, firstName, "A", lastName, new Address("", "", "", "", ""),
            "", 1L, Credit.GC, BigDecimal.ONE, BigDecimal.ZERO
        );
    }

    @Test
    public void testInsertGetFlow() {
        CustomerShared c = makeCustomer("", "l");
        assertNull(store.put(c.getKey(), c));
        assertTrue(store.containsKey(c.getKey()));
        assertThat(store.get(c.getKey()), new IdenticalCustomerMatcher(c));
    }

    @Test
    public void testInsertingAddsToNameStore() {
        CustomerShared c = makeCustomer("", "LAST");
        store.put(c.getKey(), c);
        assertThat(store.get(c.getNameKey()), new IdenticalCustomerMatcher(c));
    }

    @Test
    public void testLookupsByNameReturnMiddleCustomerSortedByFirstName() {
        CustomerShared cA = makeCustomer("A", "LAST");
        CustomerShared cB = makeCustomer("B", "LAST");
        CustomerShared cC = makeCustomer("C", "LAST");
        store.put(cA.getKey(), cA);
        store.put(cB.getKey(), cB);
        store.put(cC.getKey(), cC);

        assertThat(store.get(cA.getNameKey()), new IdenticalCustomerMatcher(cB));
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

        assertThat(store.get(cA.getNameKey()), new IdenticalCustomerMatcher(cC));
    }

    @Test
    public void testGetBlockingNameDoesntBlockIfAlreadyInStore() throws InterruptedException {
        CustomerShared c = makeCustomer("", "LAST");
        store.put(c.getKey(), c);

        Thread.currentThread().interrupt();
        // If the thread tries to block, it will throw an InterruptedException and clear interrupted
        assertThat(store.getBlocking(c.getNameKey()), new IdenticalCustomerMatcher(c));
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
                // Test will fail because nothing ended up in the map
            }
        }).start();

        assertThat(store.getBlocking(c.getNameKey()), new IdenticalCustomerMatcher(c));
    }

    @Test(expected = StoreKeyMissingException.class)
    public void testGetBlockingThrowsExceptionIfNoResultAppears() throws InterruptedException {
        store.getBlocking(new CustomerNameKey("TEST", 1, 1), 2);
    }

    private static class IdenticalCustomerMatcher extends TypeSafeDiagnosingMatcher<CustomerShared> {
        private CustomerShared expected;

        public IdenticalCustomerMatcher(CustomerShared expected) {
            this.expected = expected;
        }

        @SuppressWarnings("BigDecimalEquals")
        @Override
        protected boolean matchesSafely(CustomerShared customer, Description mismatchDescription) {
            if (customer.id != expected.id) {
                mismatchDescription.appendText("id ")
                    .appendValue(customer.id)
                    .appendText(" did not match ")
                    .appendValue(expected.id);
                return false;
            }
            if (customer.districtId != expected.districtId) {
                mismatchDescription.appendText("districtId ")
                    .appendValue(customer.districtId)
                    .appendText(" did not match ")
                    .appendValue(expected.districtId);
                return false;
            }
            if (customer.warehouseId != expected.warehouseId) {
                mismatchDescription.appendText("districtId ")
                    .appendValue(customer.warehouseId)
                    .appendText(" did not match ")
                    .appendValue(expected.warehouseId);
                return false;
            }
            if (!Objects.equals(customer.firstName, expected.firstName)) {
                mismatchDescription.appendText("firstName ")
                    .appendValue(customer.firstName)
                    .appendText(" did not match ")
                    .appendValue(expected.lastName);
                return false;
            }
            if (!Objects.equals(customer.middleName, expected.middleName)) {
                mismatchDescription.appendText("middleName ")
                    .appendValue(customer.middleName)
                    .appendText(" did not match ")
                    .appendValue(expected.lastName);
                return false;
            }
            if (!Objects.equals(customer.lastName, expected.lastName)) {
                mismatchDescription.appendText("lastName ")
                    .appendValue(customer.lastName)
                    .appendText(" did not match ")
                    .appendValue(expected.lastName);
                return false;
            }
            if (!Objects.equals(customer.address, expected.address)) {
                mismatchDescription.appendText("address ")
                    .appendValue(customer.address)
                    .appendText(" did not match ")
                    .appendValue(expected.lastName);
                return false;
            }
            if (!Objects.equals(customer.phone, expected.phone)) {
                mismatchDescription.appendText("phone ")
                    .appendValue(customer.phone)
                    .appendText(" did not match ")
                    .appendValue(expected.lastName);
                return false;
            }
            if (customer.since != expected.since) {
                mismatchDescription.appendText("since ")
                    .appendValue(customer.since)
                    .appendText(" did not match ")
                    .appendValue(expected.since);
                return false;
            }
            if (!Objects.equals(customer.credit, expected.credit)) {
                mismatchDescription.appendText("credit ")
                    .appendValue(customer.credit)
                    .appendText(" did not match ")
                    .appendValue(expected.lastName);
                return false;
            }
            if (!Objects.equals(customer.creditLimit, expected.creditLimit)) {
                mismatchDescription.appendText("creditLimit ")
                    .appendValue(customer.creditLimit)
                    .appendText(" did not match ")
                    .appendValue(expected.lastName);
                return false;
            }
            if (!Objects.equals(customer.discount, expected.discount)) {
                mismatchDescription.appendText("discount ")
                    .appendValue(customer.discount)
                    .appendText(" did not match ")
                    .appendValue(expected.lastName);
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("identical to ").appendText(expected.toString());
        }
    }
}
