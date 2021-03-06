package net.jackw.olep.acceptance;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import net.jackw.olep.edge.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.EventDatabase;
import net.jackw.olep.utils.Resetter;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.StandaloneRegistry;
import net.jackw.olep.view.ViewApp;
import net.jackw.olep.worker.WorkerApp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public abstract class BaseAcceptanceTest {
    private static Database db;
    private static DatabaseConfig config;
    private static VerifierApp verifierApp;
    private static WorkerApp workerApp;
    private static ViewApp viewApp;

    @SuppressWarnings("MustBeClosedChecker")
    public static void startDb() throws IOException, InterruptedException, ExecutionException {
        config = DatabaseConfig.create("acceptance-test");

        // Reset everything
        new Resetter(true, true, true, config).reset();

        // Start the database
        StandaloneRegistry.start();

        List<ListenableFuture<?>> futures = new ArrayList<>(4);

        verifierApp = new VerifierApp(config);
        workerApp = new WorkerApp(config);
        viewApp = new ViewApp(config);

        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        // Create futures that will resolve when the verifier and worker apps are set up and (more or less) ready to
        // process messages
        futures.add(executorService.submit(() -> {
            verifierApp.cleanup();
            verifierApp.start();
            return null;
        }));
        futures.add(executorService.submit(() -> {
            workerApp.cleanup();
            workerApp.start();
            return null;
        }));

        viewApp.start();

        futures.add(viewApp.getReadyFuture());

        // Then wait for everything to be ready...
        Futures.allAsList(futures).get();

        // Connect to the DB, and we're ready to start
        db = new EventDatabase(config);

        CurrentTestState.init(db, config, verifierApp, workerApp, viewApp);
    }

    public static void shutdown() throws InterruptedException {
        try {
            verifierApp.close();
            workerApp.close();
            viewApp.close();
            db.close();
        } finally {
            CurrentTestState.clear();
        }
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
