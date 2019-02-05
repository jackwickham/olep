package net.jackw.olep.application.transaction;

import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Scheduler;
import akka.testkit.TestProbe;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.IllegalTransactionResponseException;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionTimeoutMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.TransactionRejectedException;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.NewOrderResult;
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
public class NewOrderDispatcherTest {
    private TestProbe actor;
    private ActorSystem actorSystem;
    private MetricRegistry registry = new MetricRegistry();
    private RandomDataGenerator rand;
    private DatabaseConfig config;

    @Mock
    private Database database;

    @Mock
    private TransactionStatus<NewOrderResult> transactionStatus;

    @Captor
    private ArgumentCaptor<TransactionStatusListener<NewOrderResult>> listenerCaptor;

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
    public void testDispatchSendsNewOrderTransaction() {
        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        // Transaction shouldn't be rolled back
        when(rand.choice(1)).thenReturn(false);
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(database).newOrder(anyInt(), anyInt(), eq(4), any());
        verifyNoMoreInteractions(database);
    }

    @Test
    public void testActorNotifiedOnTransactionComplete() {
        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        when(rand.choice(1)).thenReturn(false);
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());

        listenerCaptor.getValue().completeHandler(null);

        // The actor should have been notified
        actor.expectMsgClass(TransactionCompleteMessage.class);
    }

    @Test
    public void testMetricsGatheredCorrectly() {
        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        when(rand.choice(1)).thenReturn(false);
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();
        verify(transactionStatus).register(listenerCaptor.capture());
        TransactionStatusListener<NewOrderResult> listener = listenerCaptor.getValue();

        Map<String, Timer> timers = registry.getTimers();
        Timer acceptedTimer = timers.get(MetricRegistry.name(NewOrderDispatcher.class, "accepted"));
        Timer completeTimer = timers.get(MetricRegistry.name(NewOrderDispatcher.class, "complete"));

        assertNotNull(acceptedTimer);
        assertNotNull(completeTimer);

        // So far, the timers should have been registered but not used
        assertEquals(0, acceptedTimer.getCount());
        assertEquals(0, completeTimer.getCount());

        // After delivery, nothing should have changed
        listener.deliveredHandler();
        assertEquals(0, acceptedTimer.getCount());
        assertEquals(0, completeTimer.getCount());

        // When it is accepted, only the accepted timer should be increased
        listener.acceptedHandler();
        assertEquals(1, acceptedTimer.getCount());
        assertEquals(0, completeTimer.getCount());

        // Then when it completes, the completed timer should be increased too
        listener.completeHandler(null);
        assertEquals(1, acceptedTimer.getCount());
        assertEquals(1, completeTimer.getCount());
    }

    @Test
    public void testTimeoutOccursWhenTooSlow() {
        Scheduler mockScheduler = mock(Scheduler.class);
        when(actorSystem.scheduler()).thenReturn(mockScheduler);

        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        when(rand.choice(1)).thenReturn(false);
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(mockScheduler).scheduleOnce(eq(Duration.ofSeconds(8)), eq(actor.ref()), any(TransactionTimeoutMessage.class), any(), any());
    }

    @Test
    public void testTimeoutNotCancelledByTransactionAcceptance() {
        Scheduler mockScheduler = mock(Scheduler.class);
        when(actorSystem.scheduler()).thenReturn(mockScheduler);

        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        when(rand.choice(1)).thenReturn(false);
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

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

        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        when(rand.choice(1)).thenReturn(false);
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        Cancellable scheduledEvent = mock(Cancellable.class);
        when(mockScheduler.scheduleOnce((Duration) any(), eq(actor.ref()), any(TransactionTimeoutMessage.class), any(), any())).thenReturn(scheduledEvent);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());
        listenerCaptor.getValue().completeHandler(null);

        verify(scheduledEvent).cancel();
    }

    @Test
    public void testIllegalTransactionResponseExceptionReceivedWhenTransactionRejected() {
        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        when(rand.choice(1)).thenReturn(false);
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());

        listenerCaptor.getValue().rejectedHandler(new TransactionRejectedException());

        // The actor should have been notified
        actor.expectMsgClass(IllegalTransactionResponseException.class);
    }

    @Test
    public void testTransactionRollbackWorksCorrectly() {
        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        when(rand.choice(1)).thenReturn(true); // expecting it to roll back
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());
        listenerCaptor.getValue().rejectedHandler(new TransactionRejectedException());

        actor.expectMsgClass(TransactionCompleteMessage.class);
    }

    @Test
    public void testIllegalTransactionResponseExceptionReceivedWhenRollbackTransactionDoesntFail() {
        NewOrderDispatcher dispatcher = new NewOrderDispatcher(
            4, actor.ref(), actorSystem, database, rand, config, registry
        );
        when(rand.choice(1)).thenReturn(true); // expecting it to roll back
        when(database.newOrder(anyInt(), anyInt(), eq(4), any())).thenReturn(transactionStatus);

        dispatcher.dispatch();

        verify(transactionStatus).register(listenerCaptor.capture());
        listenerCaptor.getValue().acceptedHandler();

        actor.expectMsgClass(IllegalTransactionResponseException.class);
    }
}
