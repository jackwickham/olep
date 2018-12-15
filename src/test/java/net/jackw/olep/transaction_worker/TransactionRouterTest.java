package net.jackw.olep.transaction_worker;

import com.google.common.collect.ImmutableList;
import net.jackw.olep.ForwardedMessageMatcher;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TransactionRouterTest {
    private TransactionRouter router;
    private MockProcessorContext context;

    @Before
    public void setUp() {
        router = new TransactionRouter();
        context = new MockProcessorContext();
        router.init(context);
    }

    @Test
    public void testNewOrdersRoutedToNewOrderProcessor() {
        NewOrderRequest request = new NewOrderRequest(1, 1, 1, ImmutableList.of(), 1L);

        router.process(new TransactionWarehouseKey(2L, 1), request);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.contains(
            new ForwardedMessageMatcher<>("new-order-processor", 2L, request)
        ));
    }

    @Test
    public void testPaymentsRoutedToPaymentProcessor() {
        PaymentRequest request = new PaymentRequest(1, 1, 1, 1, 1, BigDecimal.TEN);

        router.process(new TransactionWarehouseKey(3L, 1), request);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.contains(
            new ForwardedMessageMatcher<>("payment-processor", 3L, request)
        ));
    }

    @Test
    public void testDeliveryRoutedToDeliveryProcessor() {
        DeliveryRequest request = new DeliveryRequest(1, 1, 1L);

        router.process(new TransactionWarehouseKey(4L, 1), request);

        List<MockProcessorContext.CapturedForward> forwardedMessages = context.forwarded();

        assertThat(forwardedMessages, Matchers.contains(
            new ForwardedMessageMatcher<>("delivery-processor", 4L, request)
        ));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnrecognisedTransactionsResultInError() {
        TransactionRequestMessage request = new TransactionRequestMessage() {
            @Override
            public Set<Integer> getWorkerWarehouses() {
                return Set.of();
            }
        };

        router.process(new TransactionWarehouseKey(5L, 1), request);
    }
}
