package net.jackw.olep.verifier;

import com.google.common.collect.ImmutableList;
import net.jackw.olep.ForwardedMessageMatcher;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_result.ApprovalMessage;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class TransactionVerificationProcessorTest {
    private TransactionVerificationProcessor processor;
    private MockProcessorContext context;

    @Mock
    private SharedKeyValueStore<Integer, Item> itemStore;

    @Before
    public void setUp() {
        processor = new TransactionVerificationProcessor(itemStore);
        context = new MockProcessorContext();
        processor.init(context);
    }

    @Test
    public void testPaymentTransactionsAreApprovedAndForwardedToCustomersWarehouse() {
        PaymentRequest paymentRequest = new PaymentRequest(1, 1, 1, 1, 2, BigDecimal.TEN);
        processor.process(1L, paymentRequest);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.containsInAnyOrder(
            new ForwardedMessageMatcher<>(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, new TransactionWarehouseKey(1L, 2), paymentRequest),
            new ForwardedMessageMatcher<>(KafkaConfig.TRANSACTION_RESULT_TOPIC, new TransactionResultKey(1L, true), new ApprovalMessage(true))
        ));
    }

    @Test
    public void testDeliveryTransactionsAreApprovedAndForwardedToHomeWarehouse() {
        DeliveryRequest deliveryRequest = new DeliveryRequest(1, 1, 1L);
        processor.process(1L, deliveryRequest);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.containsInAnyOrder(
            new ForwardedMessageMatcher<>(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, new TransactionWarehouseKey(1L, 1), deliveryRequest),
            new ForwardedMessageMatcher<>(KafkaConfig.TRANSACTION_RESULT_TOPIC, new TransactionResultKey(1L, true), new ApprovalMessage(true))
        ));
    }

    @Test
    public void testNewOrderTransactionsAreRejectedIfAnyItemsAreMissing() {
        when(itemStore.containsKey(1)).thenReturn(true);
        when(itemStore.containsKey(2)).thenReturn(false);

        NewOrderRequest newOrderRequest = new NewOrderRequest(1, 1, 1, ImmutableList.of(
            new NewOrderRequest.OrderLine(1, 1, 1),
            new NewOrderRequest.OrderLine(2, 1, 1)
        ), 1L);
        processor.process(1L, newOrderRequest);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.contains(
            new ForwardedMessageMatcher<>(KafkaConfig.TRANSACTION_RESULT_TOPIC, new TransactionResultKey(1L, true), new ApprovalMessage(false))
        ));
    }

    @Test
    public void testNewOrderTransactionsWithValidItemsAreAcceptedAndForwardedToHomeAndSupplyingWarehouses() {
        when(itemStore.containsKey(1)).thenReturn(true);
        when(itemStore.containsKey(2)).thenReturn(true);

        NewOrderRequest newOrderRequest = new NewOrderRequest(1, 1, 1, ImmutableList.of(
            new NewOrderRequest.OrderLine(1, 2, 1),
            new NewOrderRequest.OrderLine(2, 3, 1)
        ), 1L);
        processor.process(1L, newOrderRequest);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.containsInAnyOrder(
            new ForwardedMessageMatcher<>(KafkaConfig.TRANSACTION_RESULT_TOPIC, new TransactionResultKey(1L, true), new ApprovalMessage(true)),
            new ForwardedMessageMatcher<>(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, new TransactionWarehouseKey(1L, 1), newOrderRequest),
            new ForwardedMessageMatcher<>(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, new TransactionWarehouseKey(1L, 2), newOrderRequest),
            new ForwardedMessageMatcher<>(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, new TransactionWarehouseKey(1L, 3), newOrderRequest)
        ));
    }

    @Test
    public void testUnrecognisedTransactionsRejected() {
        TransactionRequestMessage message = new TransactionRequestMessage() {
            @Override
            public Set<Integer> getWorkerWarehouses() {
                return Set.of();
            }
        };

        processor.process(1L, message);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.contains(
            new ForwardedMessageMatcher<>(KafkaConfig.TRANSACTION_RESULT_TOPIC, new TransactionResultKey(1L, true), new ApprovalMessage(false))
        ));
    }
}
