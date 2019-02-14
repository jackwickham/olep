package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestProbe;
import net.jackw.olep.application.OnDemandExecutionContext;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.utils.RandomDataGenerator;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class StockLevelDispatcherTest {
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
        config = DatabaseConfig.create(List.of());
    }

    @After
    public void shutDownAkka() {
        actorSystem.terminate();
    }

    @Test
    public void testDispatcherSendsStockLevelTransaction() {
        StockLevelDispatcher dispatcher = new StockLevelDispatcher(
            4, 8, actor.ref(), executionContext, database, rand, config
        );
        when(database.stockLevel(eq(8), eq(4), anyInt())).thenReturn(5);

        dispatcher.dispatch();
        executionContext.run();

        verify(database).stockLevel(eq(8), eq(4), anyInt());
        verifyNoMoreInteractions(database);
    }

    @Test
    public void testActorNotifiedOnTransactionComplete() {
        ActorRef actorRefSpy = spy(actor.ref());
        StockLevelDispatcher dispatcher = new StockLevelDispatcher(
            4, 8, actorRefSpy, executionContext, database, rand, config
        );
        when(database.stockLevel(eq(8), eq(4), anyInt())).thenReturn(5);

        dispatcher.dispatch();

        verify(actorRefSpy, never()).tell(any(), any());

        executionContext.run();

        verify(actorRefSpy).tell(any(TransactionCompleteMessage.class), any());
    }

    /*@Test
    public void testMetricsGatheredCorrectly() {
        StockLevelDispatcher dispatcher = new StockLevelDispatcher(
            4, 8, actor.ref(), executionContext, database, rand, config
        );
        when(database.stockLevel(eq(8), eq(4), anyInt())).then(invocation -> {
            Thread.sleep(20);
            return 5;
        });

        dispatcher.dispatch();

        Map<String, Timer> timers = registry.getTimers();
        Timer completeTimer = timers.get(MetricRegistry.name(StockLevelDispatcher.class, "complete"));

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
    }*/
}
