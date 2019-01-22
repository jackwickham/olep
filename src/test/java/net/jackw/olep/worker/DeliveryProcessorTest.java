package net.jackw.olep.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import net.jackw.olep.ForwardedMessageMatcher;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class DeliveryProcessorTest {
    private DeliveryProcessor processor;
    private MockProcessorContext context;
    private KeyValueStore<WarehouseSpecificKey, ArrayDeque<NewOrder>> newOrdersStore;
    private KeyValueStore<DistrictSpecificKey, CustomerMutable> customerMutableStore;

    @Before
    public void setUp() {
        processor = new DeliveryProcessor();
        context = new MockProcessorContext();
        newOrdersStore = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(KafkaConfig.NEW_ORDER_STORE),
            new JsonSerde<>(WarehouseSpecificKey.class),
            new JsonSerde<ArrayDeque<NewOrder>>(new TypeReference<>() {})
        ).withLoggingDisabled().build();
        newOrdersStore.init(context, newOrdersStore);
        context.register(newOrdersStore, null);

        customerMutableStore = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(KafkaConfig.CUSTOMER_MUTABLE_STORE),
            new JsonSerde<>(DistrictSpecificKey.class),
            new JsonSerde<>(CustomerMutable.class)
        ).withLoggingDisabled().build();
        customerMutableStore.init(context, customerMutableStore);
        context.register(customerMutableStore, null);

        processor.init(context);
    }

    @Test
    public void testNothingDoneIfNoNewOrdersAvailable() {
        DeliveryRequest request = new DeliveryRequest(1, 1, 1L);

        processor.process(new TransactionWarehouseKey(1L, 1), request);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.containsInAnyOrder(
            new ForwardedMessageMatcher<>(
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                new TransactionResultKey(1L, false),
                new PartialDeliveryResultMatcher(Map.of())
            )
        ));
    }

    @Test
    public void testDequeuesOrdersAndNotifiesViewsThatTheyHaveBeenDelivered() {
        DeliveryRequest request = new DeliveryRequest(1, 18, 1L);

        CustomerMutable customer = new CustomerMutable(new BigDecimal("5.55"), "datum");
        customerMutableStore.put(new DistrictSpecificKey(14, 3, 1), customer);
        customerMutableStore.put(new DistrictSpecificKey(1, 5, 1), customer);

        ArrayDeque<NewOrder> pendingOrders3 = new ArrayDeque<>(2);
        pendingOrders3.add(new NewOrder(5, 3, 1, 14, new BigDecimal("28.73")));
        pendingOrders3.add(new NewOrder(6, 3, 1, 2, BigDecimal.TEN));
        newOrdersStore.put(new WarehouseSpecificKey(3, 1), pendingOrders3);

        ArrayDeque<NewOrder> pendingOrders5 = new ArrayDeque<>(1);
        pendingOrders5.add(new NewOrder(11, 5, 1, 1, new BigDecimal("11.11")));
        newOrdersStore.put(new WarehouseSpecificKey(5, 1), pendingOrders5);

        ArrayDeque<NewOrder> pendingOrdersInOtherWarehouse = new ArrayDeque<>(1);
        pendingOrders3.add(new NewOrder(12, 1, 2, 1, BigDecimal.TEN));
        newOrdersStore.put(new WarehouseSpecificKey(1, 2), pendingOrdersInOtherWarehouse);

        processor.process(new TransactionWarehouseKey(1L, 1), request);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.containsInAnyOrder(
            new ForwardedMessageMatcher<>(
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                new TransactionResultKey(1L, false),
                new PartialDeliveryResultMatcher(Map.of(3, 5, 5, 11))
            ),
            new ForwardedMessageMatcher<>(
                KafkaConfig.MODIFICATION_LOG,
                1L,
                new DeliveryModification(5, 3, 1, 18, 1L, 14, new BigDecimal("28.73"))
            ),
            new ForwardedMessageMatcher<>(
                KafkaConfig.MODIFICATION_LOG,
                1L,
                new DeliveryModification(11, 5, 1, 18,  1L, 1, new BigDecimal("11.11"))
            )
        ));
    }

    @Test
    public void testCustomerDataUpdated() {
        DeliveryRequest request = new DeliveryRequest(1, 18, 1L);

        CustomerMutable customerDistrict3 = new CustomerMutable(new BigDecimal("5.55"), "datum");
        customerMutableStore.put(new DistrictSpecificKey(14, 3, 1), customerDistrict3);

        CustomerMutable customerDistrict5 = new CustomerMutable(new BigDecimal("-24.95"), "customerData5");
        customerMutableStore.put(new DistrictSpecificKey(1, 5, 1), customerDistrict5);

        ArrayDeque<NewOrder> pendingOrders3 = new ArrayDeque<>(2);
        pendingOrders3.add(new NewOrder(5, 3, 1, 14, new BigDecimal("28.73")));
        newOrdersStore.put(new WarehouseSpecificKey(3, 1), pendingOrders3);

        ArrayDeque<NewOrder> pendingOrders5 = new ArrayDeque<>(1);
        pendingOrders5.add(new NewOrder(11, 5, 1, 1, new BigDecimal("11.11")));
        newOrdersStore.put(new WarehouseSpecificKey(5, 1), pendingOrders5);

        processor.process(new TransactionWarehouseKey(1L, 1), request);

        CustomerMutable customer3Result = customerMutableStore.get(new DistrictSpecificKey(14, 3, 1));
        assertEquals(new BigDecimal("34.28"), customer3Result.balance);
        assertEquals("datum", customer3Result.data);

        CustomerMutable customer5Result = customerMutableStore.get(new DistrictSpecificKey(1, 5, 1));
        assertEquals(new BigDecimal("-13.84"), customer5Result.balance);
        assertEquals("customerData5", customer5Result.data);
    }

    private static class PartialDeliveryResultMatcher extends TypeSafeDiagnosingMatcher<DeliveryResult.PartialResult> {
        private Map<Integer, Integer> expectedOrders;

        public PartialDeliveryResultMatcher(Map<Integer, Integer> expectedOrders) {
            super(DeliveryResult.PartialResult.class);
            this.expectedOrders = expectedOrders;
        }

        @Override
        protected boolean matchesSafely(DeliveryResult.PartialResult item, Description mismatchDescription) {
            return expectedOrders.equals(item.processedOrders);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("PartialDeliveryResult(")
                .appendValueList("", ", ", "", expectedOrders.entrySet())
                .appendText(")");
        }
    }
}
