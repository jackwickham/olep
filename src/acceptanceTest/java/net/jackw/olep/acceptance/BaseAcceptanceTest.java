package net.jackw.olep.acceptance;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.EventDatabase;
import net.jackw.olep.utils.Resetter;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.LogViewAdapter;
import net.jackw.olep.view.StandaloneRegistry;
import net.jackw.olep.worker.WorkerApp;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class BaseAcceptanceTest {
    private Database db;
    private DatabaseConfig config;
    private VerifierApp verifierApp;
    private WorkerApp workerApp;
    private LogViewAdapter logViewAdapter;

    @Before
    @SuppressWarnings("MustBeClosedChecker")
    public void startDb() throws IOException, InterruptedException, ExecutionException {
        config = DatabaseConfig.create(getClass().getSimpleName());

        // Reset everything
        new Resetter(true, true, true, config).reset();

        // Start the database
        StandaloneRegistry.start();

        List<ListenableFuture<?>> futures = new ArrayList<>(4);

        verifierApp = new VerifierApp(config);
        workerApp = new WorkerApp(config);
        logViewAdapter = LogViewAdapter.init(
            config.getBootstrapServers(), "127.0.0.1", config
        );

        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        // Create futures that will resolve when the verifier and worker apps are set up and (more or less) ready to
        // process messages
        futures.add(executorService.submit(() -> {
            verifierApp.start();
            return null;
        }));
        futures.add(executorService.submit(() -> {
            workerApp.start();
            // We need to be able to access the stores, which means the stream threads need to be running
            // It seems to go CREATED -> RUNNING -> REBALANCING -> RUNNING before it's ready, so wait for that
            CountDownLatch readyLatch = new CountDownLatch(1);
            workerApp.addStreamStateChangeListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    readyLatch.countDown();
                }
            });
            readyLatch.await();
            return null;
        }));

        logViewAdapter.start();

        futures.add(logViewAdapter.getReadyFuture());

        // Then wait for everything to be ready...
        Futures.allAsList(futures).get();

        // Connect to the DB, and we're ready to start
        db = new EventDatabase(config.getBootstrapServers(), config.getViewRegistryHost());
    }

    @After
    public void shutdown() throws InterruptedException {
        db.close();
        verifierApp.close();
        workerApp.close();
        logViewAdapter.close();
    }

    public Database getDb() {
        return db;
    }

    public DatabaseConfig getConfig() {
        return config;
    }

    public WorkerApp getWorkerApp() {
        return workerApp;
    }
}
