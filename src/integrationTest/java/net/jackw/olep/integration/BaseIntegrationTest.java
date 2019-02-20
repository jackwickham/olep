package net.jackw.olep.integration;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.worker.WorkerApp;
import net.jackw.olep.utils.Resetter;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.StandaloneRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class BaseIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(120);

    private static DatabaseConfig config;

    /**
     * Set up the immutable stuff once per class (JUnit doesn't really allow once per suite)
     */
    @BeforeClass
    public static void resetImmutableTopics() throws InterruptedException, ExecutionException, IOException {
        config = DatabaseConfig.create("BaseIntegrationTest");
        new Resetter(true, false, true, config).reset();
    }

    @Before
    public void resetMutableTopics() throws InterruptedException, ExecutionException {
        new Resetter(false, true, true, config).reset();
    }

    @BeforeClass
    public static void ensureRegistryRunning() {
        StandaloneRegistry.start();
    }

    protected String getEventBootsrapServers() {
        return config.getBootstrapServers();
    }

    protected String getViewBootstrapServers() {
        return config.getViewRegistryHost();
    }

    protected static int getCustomerNameRange() {
        return 5;
    }

    private List<VerifierApp> verifierInstances = new ArrayList<>();
    private List<WorkerApp> workerInstances = new ArrayList<>();
    private LogViewAdapterStub logViewAdapter = null;

    /**
     * Start a verifier instance, with a fresh state store
     */
    protected void startVerifier() throws Exception {
        VerifierApp verifier = new VerifierApp(config);
        verifierInstances.add(verifier);
        verifier.start();
    }

    protected void startWorker() throws Exception {
        WorkerApp worker = new WorkerApp(config);
        workerInstances.add(worker);
        worker.start();
    }

    @SuppressWarnings("MustBeClosedChecker")
    protected void startView() {
        logViewAdapter = LogViewAdapterStub.create(config.getViewRegistryHost());
    }

    @After
    public void stopWorkers() throws InterruptedException {
        for (VerifierApp verifier : verifierInstances) {
            verifier.close();
        }
        if (!verifierInstances.isEmpty()) {
            verifierInstances.get(0).cleanup();
        }

        for (WorkerApp worker : workerInstances) {
            worker.close();
        }
        if (!workerInstances.isEmpty()) {
            workerInstances.get(0).cleanup();
        }

        if (logViewAdapter != null) {
            logViewAdapter.close();
        }
    }
}
