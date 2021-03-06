package net.jackw.olep.application.transaction;

import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Scheduler;
import akka.testkit.TestProbe;
import net.jackw.olep.application.IllegalTransactionResponseException;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionTimeoutMessage;
import net.jackw.olep.edge.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.TransactionRejectedException;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.DeliveryResult;
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

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeliveryDispatcherTest {
    private TestProbe actor;

    private ActorSystem actorSystem;

    private DatabaseConfig config;

    @Mock
    private Database database;

    @Mock
    private TransactionStatus<DeliveryResult> transactionStatus;

    @Captor
    private ArgumentCaptor<TransactionStatusListener<DeliveryResult>> listenerCaptor;

    @Before
    public void setupAkka() {
        actorSystem = spy(ActorSystem.create());
        actor = new TestProbe(actorSystem);
    }

    @Before
    public void loadConfigFile() throws IOException {
        config = DatabaseConfig.create("DeliveryDispatcherTest");
    }

    @After
    public void shutDownAkka() {
        actorSystem.terminate();
    }

    @Test
    public void testDispatchSendsDeliveryTransaction() {
        DeliveryDispatcher dispatcher = new DeliveryDispatcher(
            4, actor.ref(), actorSystem, database, new RandomDataGenerator(0), config
        );

        when(database.delivery(eq(4), anyInt())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(database).delivery(eq(4), anyInt());
        verifyNoMoreInteractions(database);
    }

    @Test
    public void testActorNotifiedOnTransactionAccepted() {
        DeliveryDispatcher dispatcher = new DeliveryDispatcher(
            4, actor.ref(), actorSystem, database, new RandomDataGenerator(0), config
        );

        when(database.delivery(eq(4), anyInt())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());

        // Delivery is done when accepted rather than when completed
        listenerCaptor.getValue().acceptedHandler();

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

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(
            4, actor.ref(), actorSystem, database, new RandomDataGenerator(0), mockConfig
        );

        when(database.delivery(eq(4), anyInt())).thenReturn(transactionStatus);

        dispatcher.dispatch();
        verify(transactionStatus).register(listenerCaptor.capture());
        TransactionStatusListener<DeliveryResult> listener = listenerCaptor.getValue();

        // So far, the timer should have been started but no metrics should have been recorded
        verify(mockMetrics, times(1)).startTimer();
        verifyNoMoreInteractions(mockMetrics);

        // After delivery, no metrics should have been recorded yet
        listener.deliveredHandler();
        verifyNoMoreInteractions(mockMetrics);

        // When it is accepted, only the accepted timer should be increased
        listener.acceptedHandler();
        verify(mockMetrics, times(1)).recordDuration(DurationType.DELIVERY_ACCEPTED, mockTimer);
        verifyNoMoreInteractions(mockMetrics);

        // Then when it completes, the completed timer should be increased too
        listener.completeHandler(null);
        verify(mockMetrics, times(1)).recordDuration(DurationType.DELIVERY_COMPLETE, mockTimer);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void testTimeoutOccursWhenTooSlow() {
        Scheduler mockScheduler = mock(Scheduler.class);
        when(actorSystem.scheduler()).thenReturn(mockScheduler);

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(
            4, actor.ref(), actorSystem, database, new RandomDataGenerator(0), config
        );

        when(database.delivery(eq(4), anyInt())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(mockScheduler).scheduleOnce(eq(Duration.ofSeconds(90)), eq(actor.ref()), any(TransactionTimeoutMessage.class), any(), any());
    }

    @Test
    public void testTimeoutNotCancelledByTransactionAcceptance() {
        Scheduler mockScheduler = mock(Scheduler.class);
        when(actorSystem.scheduler()).thenReturn(mockScheduler);

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(
            4, actor.ref(), actorSystem, database, new RandomDataGenerator(0), config
        );

        when(database.delivery(eq(4), anyInt())).thenReturn(transactionStatus);

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

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(
            4, actor.ref(), actorSystem, database, new RandomDataGenerator(0), config
        );

        when(database.delivery(eq(4), anyInt())).thenReturn(transactionStatus);

        Cancellable scheduledEvent = mock(Cancellable.class);
        when(mockScheduler.scheduleOnce((Duration) any(), eq(actor.ref()), any(TransactionTimeoutMessage.class), any(), any())).thenReturn(scheduledEvent);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());
        listenerCaptor.getValue().completeHandler(null);

        verify(scheduledEvent).cancel();
    }

    @Test
    public void testIllegalTransactionResponseExceptionReceivedWhenTransactionRejected() {
        DeliveryDispatcher dispatcher = new DeliveryDispatcher(
            4, actor.ref(), actorSystem, database, new RandomDataGenerator(0), config
        );

        when(database.delivery(eq(4), anyInt())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());

        listenerCaptor.getValue().rejectedHandler(new TransactionRejectedException());

        // The actor should have been notified
        actor.expectMsgClass(IllegalTransactionResponseException.class);
    }
}
