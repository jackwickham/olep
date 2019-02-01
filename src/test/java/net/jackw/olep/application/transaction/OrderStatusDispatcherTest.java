package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestProbe;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.OnDemandExecutionContext;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.utils.RandomDataGenerator;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class OrderStatusDispatcherTest {
    private TestProbe actor;
    private ActorSystem actorSystem;
    private MetricRegistry registry = new MetricRegistry();
    private RandomDataGenerator rand;
    private OnDemandExecutionContext executionContext = new OnDemandExecutionContext();

    @Mock
    private Database database;

    @Before
    public void setupAkka() {
        actorSystem = spy(ActorSystem.create());
        actor = new TestProbe(actorSystem);
        rand = spy(new RandomDataGenerator(0));
    }

    @After
    public void shutDownAkka() {
        actorSystem.terminate();
    }

    @Test
    public void testDispatcherSendsOrderStatusTransactionById() {
        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actor.ref(), executionContext, database, rand, registry
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
            4, actor.ref(), executionContext, database, rand, registry
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
            4, actorRefSpy, executionContext, database, rand, registry
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
            4, actorRefSpy, executionContext, database, rand, registry
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
        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actor.ref(), executionContext, database, rand, registry
        );
        when(rand.choice(60)).thenReturn(false);
        when(database.orderStatus(anyInt(), anyInt(), eq(4))).then(invocation -> {
            Thread.sleep(20);
            return null;
        });

        dispatcher.dispatch();

        Map<String, Timer> timers = registry.getTimers();
        Timer completeTimer = timers.get(MetricRegistry.name(OrderStatusDispatcher.class, "complete"));

        assertNotNull(completeTimer);

        // The transaction should be incomplete, so the timer should not have been triggered
        assertEquals(0, completeTimer.getCount());

        // Run the task
        long startTime = System.nanoTime();
        executionContext.run();
        long duration = System.nanoTime() - startTime;

        // Once the transaction runs successfully, the timer should have been run
        assertEquals(1, completeTimer.getCount());
        // and the duration should be less than the whole set of tasks took to run, but greater than the 20ms that we
        // pretended the transaction took
        assertThat(
            completeTimer.getSnapshot().getValues()[0],
            Matchers.both(Matchers.lessThan(duration)).and(Matchers.greaterThanOrEqualTo(20000000L))
        );
    }

    @Test
    public void testMetricsGatheredCorrectlyWhenByName() {
        OrderStatusDispatcher dispatcher = new OrderStatusDispatcher(
            4, actor.ref(), executionContext, database, rand, registry
        );
        when(rand.choice(60)).thenReturn(true);
        when(database.orderStatus(any(String.class), anyInt(), eq(4))).then(invocation -> {
            Thread.sleep(20);
            return null;
        });

        dispatcher.dispatch();

        Map<String, Timer> timers = registry.getTimers();
        Timer completeTimer = timers.get(MetricRegistry.name(OrderStatusDispatcher.class, "complete"));

        assertNotNull(completeTimer);

        // The transaction should be incomplete, so the timer should not have been triggered
        assertEquals(0, completeTimer.getCount());

        // Run the task
        long startTime = System.nanoTime();
        executionContext.run();
        long duration = System.nanoTime() - startTime;

        // Once the transaction runs successfully, the timer should have been run
        assertEquals(1, completeTimer.getCount());
        // and the duration should be less than the whole set of tasks took to run, but greater than the 20ms that we
        // pretended the transaction took
        assertThat(
            completeTimer.getSnapshot().getValues()[0],
            Matchers.both(Matchers.lessThan(duration)).and(Matchers.greaterThanOrEqualTo(20000000L))
        );
    }
}
