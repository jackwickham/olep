package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestProbe;
import net.jackw.olep.application.OnDemandExecutionContext;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
import net.jackw.olep.utils.RandomDataGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class OrderStatusDispatcherTest {
    private TestProbe actor;
    private ActorSystem actorSystem;
    private RandomDataGenerator rand;
    private OnDemandExecutionContext executionContext = new OnDemandExecutionContext();
    private DatabaseConfig config;

    @Mock
    private Database database;

    @Before
    public void setupAkka() {
        actorSystem = spy(ActorSystem.create());
        actor = new TestProbe(actorSystem);
        rand = spy(new RandomDataGenerator(0));
    }

    @Before
    public void loadConfigFile() throws IOException {
        config = DatabaseConfig.create();
    }

    @After
    public void shutDownAkka() {
        actorSystem.terminate();
    }

    @Test
    public void testDispatcherSendsOrderStatusTransactionById() {
        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actor.ref(), executionContext, database, rand, config
        );
        // Send transaction by ID
        when(rand.choice(60)).thenReturn(false);
        when(database.orderStatus(anyInt(), anyInt(), eq(4))).thenReturn(null);

        dispatcher.dispatch();
        executionContext.run();

        verify(database).orderStatus(anyInt(), anyInt(), eq(4));
        verifyNoMoreInteractions(database);
    }

    @Test
    public void testDispatcherSendsOrderStatusTransactionByName() {
        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actor.ref(), executionContext, database, rand, config
        );
        // Send transaction by name
        when(rand.choice(60)).thenReturn(true);
        when(database.orderStatus(any(String.class), anyInt(), eq(4))).thenReturn(null);

        dispatcher.dispatch();
        executionContext.run();

        verify(database).orderStatus(any(String.class), anyInt(), eq(4));
        verifyNoMoreInteractions(database);
    }

    @Test
    public void testActorNotifiedOnTransactionCompleteById() {
        ActorRef actorRefSpy = spy(actor.ref());
        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actorRefSpy, executionContext, database, rand, config
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.orderStatus(anyInt(), anyInt(), eq(4))).thenReturn(null);

        dispatcher.dispatch();

        verify(actorRefSpy, never()).tell(any(), any());

        executionContext.run();

        verify(actorRefSpy).tell(any(TransactionCompleteMessage.class), any());
    }

    @Test
    public void testActorNotifiedOnTransactionCompleteByName() {
        ActorRef actorRefSpy = spy(actor.ref());
        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actorRefSpy, executionContext, database, rand, config
        );
        when(rand.choice(60)).thenReturn(true);
        when(database.orderStatus(any(String.class), anyInt(), eq(4))).thenReturn(null);

        dispatcher.dispatch();

        verify(actorRefSpy, never()).tell(any(), any());

        executionContext.run();

        verify(actorRefSpy).tell(any(TransactionCompleteMessage.class), any());
    }

    @Test
    public void testMetricsGatheredCorrectlyWhenById() {
        DatabaseConfig mockConfig = spy(config);
        Metrics mockMetrics = mock(Metrics.class);
        Timer mockTimer = mock(Timer.class);

        when(mockConfig.getMetrics()).thenReturn(mockMetrics);
        when(mockMetrics.startTimer()).thenReturn(mockTimer);

        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actor.ref(), executionContext, database, rand, mockConfig
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.orderStatus(anyInt(), anyInt(), eq(4))).then(invocation -> {
            // The timer should have started but not finished
            verify(mockMetrics, times(1)).startTimer();
            verifyNoMoreInteractions(mockMetrics);
            return null;
        });

        dispatcher.dispatch();

        // The timer shouldn't have started yet, because it's run in a separate thread
        verifyNoMoreInteractions(mockMetrics);

        // Run the task
        executionContext.run();

        // Once the transaction runs successfully, the timer should be complete
        verify(mockMetrics, times(1)).recordDuration(DurationType.ORDER_STATUS_COMPLETE, mockTimer);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void testMetricsGatheredCorrectlyWhenByName() {
        DatabaseConfig mockConfig = spy(config);
        Metrics mockMetrics = mock(Metrics.class);
        Timer mockTimer = mock(Timer.class);

        when(mockConfig.getMetrics()).thenReturn(mockMetrics);
        when(mockMetrics.startTimer()).thenReturn(mockTimer);

        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actor.ref(), executionContext, database, rand, mockConfig
        );
        when(rand.choice(60)).thenReturn(true);
        when(database.orderStatus(any(String.class), anyInt(), eq(4))).then(invocation -> {
            // The timer should have started but not finished
            verify(mockMetrics, times(1)).startTimer();
            verifyNoMoreInteractions(mockMetrics);
            return null;
        });

        dispatcher.dispatch();

        // The timer shouldn't have started yet, because it's run in a separate thread
        verifyNoMoreInteractions(mockMetrics);

        // Run the task
        executionContext.run();

        // Once the transaction runs successfully, the timer should be complete
        verify(mockMetrics, times(1)).recordDuration(DurationType.ORDER_STATUS_COMPLETE, mockTimer);
        verifyNoMoreInteractions(mockMetrics);
    }
}
