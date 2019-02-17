package net.jackw.olep.application.transaction;

import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Scheduler;
import akka.testkit.TestProbe;
import net.jackw.olep.application.IllegalTransactionResponseException;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionTimeoutMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.TransactionRejectedException;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
import net.jackw.olep.utils.RandomDataGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PaymentDispatcherTest {
    private TestProbe actor;
    private ActorSystem actorSystem;
    private RandomDataGenerator rand;
    private DatabaseConfig config;

    @Mock
    private Database database;

    @Mock
    private TransactionStatus<PaymentResult> transactionStatus;

    @Captor
    private ArgumentCaptor<TransactionStatusListener<PaymentResult>> listenerCaptor;

    @Before
    public void setupAkka() {
        actorSystem = spy(ActorSystem.create());
        actor = new TestProbe(actorSystem);
        rand = spy(new RandomDataGenerator(0));
    }

    @Before
    public void loadConfigFile() throws IOException {
        config = DatabaseConfig.create(List.of());
    }

    @After
    public void shutDownAkka() {
        actorSystem.terminate();
    }

    @Test
    public void testDispatchSendsPaymentTransactionById() {
        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );

        // Ensure that it uses the customer's ID
        when(rand.choice(60)).thenReturn(false);

        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), anyInt(), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(database).payment(anyInt(), anyInt(), eq(4), anyInt(), anyInt(), any());
        verifyNoMoreInteractions(database);
    }

    @Test
    public void testDispatchSendsPaymentTransactionByName() {
        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );

        // Ensure that it uses the customer's name
        when(rand.choice(60)).thenReturn(true);

        when(database.payment(any(String.class), anyInt(), eq(4), anyInt(), anyInt(), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(database).payment(any(String.class), anyInt(), eq(4), anyInt(), anyInt(), any());
        verifyNoMoreInteractions(database);
    }

    @Test
    public void testActorNotifiedOnTransactionComplete() {
        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), anyInt(), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());

        listenerCaptor.getValue().completeHandler(null);

        // The actor should have been notified
        actor.expectMsgClass(TransactionCompleteMessage.class);
    }

    @Test
    public void testMetricsGatheredCorrectly() {
        DatabaseConfig mockConfig = spy(config);
        Metrics mockMetrics = mock(Metrics.class);
        Timer mockTimer = mock(Timer.class);

        when(mockConfig.getMetrics()).thenReturn(mockMetrics);
        when(mockMetrics.startTimer()).thenReturn(mockTimer);

        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, mockConfig
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), anyInt(), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();
        verify(transactionStatus).register(listenerCaptor.capture());
        TransactionStatusListener<PaymentResult> listener = listenerCaptor.getValue();

        // So far, the timer should have been started but no metrics should have been recorded
        verify(mockMetrics, times(1)).startTimer();
        verifyNoMoreInteractions(mockMetrics);

        // After delivery, no metrics should have been recorded yet
        listener.deliveredHandler();
        verifyNoMoreInteractions(mockMetrics);

        // When it is accepted, only the accepted timer should be increased
        listener.acceptedHandler();
        verify(mockMetrics, times(1)).recordDuration(DurationType.PAYMENT_ACCEPTED, mockTimer);
        verifyNoMoreInteractions(mockMetrics);

        // Then when it completes, the completed timer should be increased too
        listener.completeHandler(null);
        verify(mockMetrics, times(1)).recordDuration(DurationType.PAYMENT_COMPLETE, mockTimer);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void testTimeoutOccursWhenTooSlow() {
        Scheduler mockScheduler = mock(Scheduler.class);
        when(actorSystem.scheduler()).thenReturn(mockScheduler);

        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), anyInt(), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(mockScheduler).scheduleOnce(eq(Duration.ofSeconds(90)), eq(actor.ref()), any(TransactionTimeoutMessage.class), any(), any());
    }

    @Test
    public void testTimeoutNotCancelledByTransactionAcceptance() {
        Scheduler mockScheduler = mock(Scheduler.class);
        when(actorSystem.scheduler()).thenReturn(mockScheduler);

        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), anyInt(), any())).thenReturn(transactionStatus);

        Cancellable scheduledEvent = mock(Cancellable.class);
        when(mockScheduler.scheduleOnce((Duration) any(), eq(actor.ref()), any(TransactionTimeoutMessage.class), any(), any())).thenReturn(scheduledEvent);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());
        listenerCaptor.getValue().acceptedHandler();

        verify(scheduledEvent, never()).cancel();
    }

    @Test
    public void testTimeoutCancelledByTransactionComplete() {
        Scheduler mockScheduler = mock(Scheduler.class);
        when(actorSystem.scheduler()).thenReturn(mockScheduler);

        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), anyInt(), any())).thenReturn(transactionStatus);

        Cancellable scheduledEvent = mock(Cancellable.class);
        when(mockScheduler.scheduleOnce((Duration) any(), eq(actor.ref()), any(TransactionTimeoutMessage.class), any(), any())).thenReturn(scheduledEvent);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());
        listenerCaptor.getValue().completeHandler(null);

        verify(scheduledEvent).cancel();
    }

    @Test
    public void testIllegalTransactionResponseExceptionReceivedWhenTransactionRejected() {
        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), anyInt(), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());

        listenerCaptor.getValue().rejectedHandler(new TransactionRejectedException());

        // The actor should have been notified
        actor.expectMsgClass(IllegalTransactionResponseException.class);
    }

    @Test
    public void testUsesHomeWarehouseWhenNecessary() {
        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );

        // Ensure that it uses the customer's ID
        when(rand.choice(60)).thenReturn(false);
        when(rand.choice(15)).thenReturn(false);

        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(database).payment(anyInt(), anyInt(), eq(4), anyInt(), eq(4), any());
        verifyNoMoreInteractions(database);
    }

    @Test
    public void testUsesRemoteWarehouseWhenNecessary() {
        PaymentDispatcher dispatcher = new PaymentDispatcher(
            4, actor.ref(), actorSystem, database, rand, config
        );

        // Ensure that it uses the customer's ID
        when(rand.choice(60)).thenReturn(false);
        when(rand.choice(15)).thenReturn(true);
        when(rand.uniform(1, config.getWarehouseCount())).thenReturn(50000);

        when(database.payment(anyInt(), anyInt(), eq(4), anyInt(), eq(50000), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(database).payment(anyInt(), anyInt(), eq(4), anyInt(), eq(50000), any());
        verifyNoMoreInteractions(database);
    }
}
