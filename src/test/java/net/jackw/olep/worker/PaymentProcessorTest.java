package net.jackw.olep.worker;

import com.google.common.base.Strings;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.DistrictSpecificKeySerde;
import net.jackw.olep.common.store.SharedCustomerStore;
import net.jackw.olep.common.store.SharedKeyValueStore;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.ModificationKey;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import net.jackw.olep.metrics.InMemoryMetrics;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PaymentProcessorTest {
    private PaymentProcessor processor;
    private MockProcessorContext context;
    private KeyValueStore<DistrictSpecificKey, CustomerMutable> customerMutableStore;

    @Mock
    private SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore;
    @Mock
    private SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore;
    @Mock
    private SharedCustomerStore customerImmutableStore;

    private WarehouseShared warehouseShared;
    private DistrictShared districtShared;
    private CustomerShared customerShared;
    private CustomerMutable customerMutable;

    @Before
    public void setUp() throws InterruptedException {
        processor = new PaymentProcessor(warehouseImmutableStore, districtImmutableStore, customerImmutableStore, new InMemoryMetrics());
        context = new MockProcessorContext();
        customerMutableStore = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(KafkaConfig.CUSTOMER_MUTABLE_STORE),
            DistrictSpecificKeySerde.getInstance(),
            new JsonSerde<>(CustomerMutable.class)
        ).withLoggingDisabled().build();
        customerMutableStore.init(context, customerMutableStore);
        context.register(customerMutableStore, null);

        processor.init(context);

        // Populate stores
        // Warehouse
        warehouseShared = new WarehouseShared(
            2, "WH1", makeAddress("w"), new BigDecimal("0.15")
        );
        when(warehouseImmutableStore.getBlocking(2)).thenReturn(warehouseShared);

        // District
        districtShared = new DistrictShared(
            1, 2, "D2", makeAddress("d"), new BigDecimal("0.077")
        );
        when(districtImmutableStore.getBlocking(new WarehouseSpecificKey(1, 2))).thenReturn(districtShared);

    }

    @After
    public void tearDown() {
        customerMutableStore.close();
    }

    private void insertCustomer(boolean goodCredit) throws InterruptedException {
        // Customer
        customerShared = new CustomerShared(
            3, 4, 5, "FIRST", "MIDDLE", "LAST", makeAddress("c"),
            "1234", 500L, goodCredit ? Credit.GC : Credit.BC, new BigDecimal("101.01"), new BigDecimal("0.03")
        );
        customerMutable = new CustomerMutable(
            new BigDecimal("12.34"), "|" + Strings.repeat("d", 499)
        );
        when(customerImmutableStore.getBlocking(customerShared.getKey())).thenReturn(customerShared);
        when(customerImmutableStore.getBlocking(customerShared.getNameKey())).thenReturn(customerShared);
        customerMutableStore.put(customerShared.getKey(), customerMutable);
    }

    @Test
    public void testAllFieldsSetCorrectlyInTransactionResult() throws InterruptedException {
        insertCustomer(true);

        // Run the transaction
        PaymentRequest request = new PaymentRequest(3, 1, 2, 4, 5, new BigDecimal("67.89"));
        processor.process(new TransactionWarehouseKey(10L, 2), request);

        // And test that it did the right thing
        List<MockProcessorContext.CapturedForward> forwards = context.forwarded();

        verifyForwards(forwards);
    }

    @Test
    public void testWhenBadCreditTheDataIsUpdated() throws InterruptedException {
        insertCustomer(false);

        // Now we can actually run it
        PaymentRequest request = new PaymentRequest(3, 1, 2, 4, 5, new BigDecimal("67.89"));
        processor.process(new TransactionWarehouseKey(10L, 2), request);

        // And test that it did the right thing
        List<MockProcessorContext.CapturedForward> forwards = context.forwarded();

        verifyForwards(forwards);
    }

    @Test
    public void testLookingUpCustomerByNameWorks() throws InterruptedException {
        insertCustomer(true);

        // Now we can actually run it
        PaymentRequest request = new PaymentRequest("LAST", 1, 2, 4, 5, new BigDecimal("67.89"));
        processor.process(new TransactionWarehouseKey(10L, 2), request);

        // And test that it did the right thing
        List<MockProcessorContext.CapturedForward> forwards = context.forwarded();
        verifyForwards(forwards);
    }

    private void verifyForwards(List<MockProcessorContext.CapturedForward> forwards) {
        PaymentResult.PartialResult resultMessage = null;
        PaymentModification modificationMessage = null;

        assertThat(forwards, Matchers.hasSize(2));

        // Get the two messages - one should be a result and the other a modification
        for (MockProcessorContext.CapturedForward forward : forwards) {
            if (forward.childName().equals(KafkaConfig.TRANSACTION_RESULT_TOPIC)) {
                // Make sure this looks like a transaction result, and the key is correct
                assertEquals(new TransactionResultKey(10L, false), forward.keyValue().key);
                assertThat(forward.keyValue().value, Matchers.instanceOf(PaymentResult.PartialResult.class));
                resultMessage = (PaymentResult.PartialResult) forward.keyValue().value;
            } else if (forward.childName().equals(KafkaConfig.MODIFICATION_LOG)) {
                // Make sure this looks like a modification, and the key is correct
                assertEquals(new ModificationKey(10L, (short) 0), forward.keyValue().key);
                assertThat(forward.keyValue().value, Matchers.instanceOf(PaymentModification.class));
                modificationMessage = (PaymentModification) forward.keyValue().value;
            } else {
                throw new AssertionError("Message forwarded to unexpected topic " + forward.childName());
            }
        }
        // Check we got both messages
        assertNotNull(resultMessage);
        assertNotNull(modificationMessage);

        // Now make sure the messages are what we expected
        assertEquals(warehouseShared.address, resultMessage.warehouseAddress);
        assertEquals(districtShared.address, resultMessage.districtAddress);
        assertEquals(customerShared.id, (int) resultMessage.customerId);
        assertEquals(customerShared.firstName, resultMessage.customerFirstName);
        assertEquals(customerShared.middleName, resultMessage.customerMiddleName);
        assertEquals(customerShared.lastName, resultMessage.customerLastName);
        assertEquals(customerShared.address, resultMessage.customerAddress);
        assertEquals(customerShared.credit, resultMessage.customerCredit);
        assertEquals(customerShared.creditLimit, resultMessage.customerCreditLimit);
        assertEquals(customerShared.discount, resultMessage.customerDiscount);
        assertEquals(new BigDecimal("-55.55"), resultMessage.customerBalance);
        String newCustomerData;
        if (customerShared.credit == Credit.GC) {
            assertNull(resultMessage.customerData);
            newCustomerData = customerMutable.data;
        } else {
            newCustomerData = customerMutableStore.get(customerShared.getKey()).data;
            assertThat(newCustomerData, new CustomerDataMatcher(
                3, 4, 5, 1, 2,
                new BigDecimal("67.89"), customerMutable.data
            ));
            assertEquals(newCustomerData.substring(0, 200), resultMessage.customerData);
        }
        assertEquals(warehouseShared.id, modificationMessage.warehouseId);
        assertEquals(districtShared.id, modificationMessage.districtId);
        assertEquals(customerShared.id, modificationMessage.customerId);
        assertEquals(customerShared.warehouseId, modificationMessage.customerWarehouseId);
        assertEquals(customerShared.districtId, modificationMessage.customerDistrictId);
        assertEquals(new BigDecimal("67.89"), modificationMessage.amount);
        assertEquals(new BigDecimal("-55.55"), modificationMessage.balance);
        assertEquals(newCustomerData, modificationMessage.customerData);

        // And make sure the store has been correctly updated
        assertEquals(new BigDecimal("-55.55"), customerMutableStore.get(customerShared.getKey()).balance);
        assertEquals(newCustomerData, customerMutableStore.get(customerShared.getKey()).data);
    }

    private Address makeAddress(String prefix) {
        return new Address(prefix + "s1", prefix + "s2", prefix + "c", prefix + "st", prefix + "z");
    }

    private static class CustomerDataMatcher extends TypeSafeDiagnosingMatcher<String> {
        private int customerId;
        private int customerDistrictId;
        private int customerWarehouseId;
        private int districtId;
        private int warehouseId;
        private BigDecimal amount;
        private String previousValue;

        public CustomerDataMatcher(
            int customerId, int customerDistrictId, int customerWarehouseId, int districtId, int warehouseId,
            BigDecimal amount, String previousValue
        ) {
            super(String.class);
            this.customerId = customerId;
            this.customerDistrictId = customerDistrictId;
            this.customerWarehouseId = customerWarehouseId;
            this.districtId = districtId;
            this.warehouseId = warehouseId;
            this.amount = amount;
            this.previousValue = previousValue;
        }

        @Override
        protected boolean matchesSafely(String item, Description mismatchDescription) {
            mismatchDescription.appendValue(item);
            int custIdEnd = item.indexOf(Integer.toString(customerId));
            if (custIdEnd == -1) {
                mismatchDescription.appendText(" did not contain customer id ")
                    .appendValue(customerId);
                return false;
            }
            int custDistIdEnd = item.indexOf(Integer.toString(customerDistrictId), custIdEnd + 1);
            if (custDistIdEnd == -1) {
                mismatchDescription.appendText(" did not contain customer district id ")
                    .appendValue(customerDistrictId)
                    .appendText("after index")
                    .appendValue(custIdEnd);
                return false;
            }
            int custWhIdEnd = item.indexOf(Integer.toString(customerWarehouseId), custDistIdEnd + 1);
            if (custWhIdEnd == -1) {
                mismatchDescription.appendText(" did not contain customer warehouse id ")
                    .appendValue(customerWarehouseId)
                    .appendText("after index")
                    .appendValue(custDistIdEnd);
                return false;
            }
            int distIdEnd = item.indexOf(Integer.toString(districtId), custWhIdEnd + 1);
            if (distIdEnd == -1) {
                mismatchDescription.appendText(" did not contain district id ")
                    .appendValue(districtId)
                    .appendText("after index")
                    .appendValue(custWhIdEnd);
                return false;
            }
            int whIdEnd = item.indexOf(Integer.toString(warehouseId), distIdEnd + 1);
            if (whIdEnd == -1) {
                mismatchDescription.appendText(" did not contain warehouse id ")
                    .appendValue(warehouseId)
                    .appendText("after index")
                    .appendValue(distIdEnd);
                return false;
            }
            int amountEnd = item.indexOf(amount.toString(), whIdEnd + 1);
            if (amountEnd == -1) {
                mismatchDescription.appendText(" did not contain amount ")
                    .appendValue(amount)
                    .appendText("after index")
                    .appendValue(whIdEnd);
                return false;
            }
            int existingData = item.indexOf(previousValue.substring(0, 5), amountEnd + 1);
            if (existingData == -1) {
                mismatchDescription.appendText(" did not end with existing data ");
                return false;
            }
            if (item.length() != 500) {
                mismatchDescription.appendText(" was ").appendValue(item.length()).appendText(" characters long");
                return false;
            }
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void describeTo(Description description) {
            description.appendValueList(
                "customerData(", ", ", ")",
                customerId, customerDistrictId, customerWarehouseId, districtId, warehouseId, amount
            );
        }
    }
}
