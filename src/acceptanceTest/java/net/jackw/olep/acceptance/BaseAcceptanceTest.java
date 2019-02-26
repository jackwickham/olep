package net.jackw.olep.acceptance;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.EventDatabase;
import net.jackw.olep.utils.Resetter;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.LogViewAdapter;
import net.jackw.olep.view.StandaloneRegistry;
import net.jackw.olep.worker.WorkerApp;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class BaseAcceptanceTest {
    private Database db;
    private DatabaseConfig config;

    @Before
    @SuppressWarnings("MustBeClosedChecker")
    public void startDb() throws IOException, InterruptedException, ExecutionException {
        config = DatabaseConfig.create(getClass().getSimpleName());

        // Reset everything
        new Resetter(true, true, true, config).reset();

        // Start the database
        StandaloneRegistry.start();

        List<ListenableFuture<?>> futures = Collections.synchronizedList(new ArrayList<>(3));
        CountDownLatch threadStartLatch = new CountDownLatch(2);

        new Thread(() -> {
            VerifierApp app = new VerifierApp(config);
            futures.add(app.getBeforeStartFuture());
            threadStartLatch.countDown();
            app.run();
        }, "verifier-main").start();
        new Thread(() -> {
            WorkerApp app = new WorkerApp(config);
            futures.add(app.getBeforeStartFuture());
            threadStartLatch.countDown();
            app.run();
        }, "worker-main").start();

        LogViewAdapter logViewAdapter = LogViewAdapter.init(config.getBootstrapServers(), config.getViewRegistryHost(), config);
        logViewAdapter.start();

        futures.add(logViewAdapter.getReadyFuture());
        // Wait for all the threads to have started and registered their futures
        threadStartLatch.await();

        // Then wait for everything to be ready
        Futures.allAsList(futures).get();

        // Connect to the DB, and we're ready to start
        db = new EventDatabase(config.getBootstrapServers(), config.getViewRegistryHost());
    }

    @After
    public void shutdown() {
        db.close();
    }

    public Database getDb() {
        return db;
    }

    public DatabaseConfig getConfig() {
        return config;
    }
}
