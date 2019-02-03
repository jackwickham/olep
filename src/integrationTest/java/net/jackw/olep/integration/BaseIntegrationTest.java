package net.jackw.olep.integration;

import net.jackw.olep.worker.WorkerApp;
import net.jackw.olep.utils.Resetter;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.StandaloneRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class BaseIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(120);

    /**
     * Set up the immutable stuff once per class (JUnit doesn't really allow once per suite)
     */
    @BeforeClass
    public static void resetImmutableTopics() throws InterruptedException, ExecutionException {
        new Resetter(true, false, true).reset();
    }

    @Before
    public void resetMutableTopics() throws InterruptedException, ExecutionException {
        new Resetter(false, true, true).reset();
    }

    @BeforeClass
    public static void ensureRegistryRunning() {
        StandaloneRegistry.start();
    }

    protected String getEventBootsrapServers() {
        return "127.0.0.1:9092";
    }

    protected String getViewBootstrapServers() {
        return "127.0.0.1";
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
    protected void startVerifier() {
        VerifierApp verifier = new VerifierApp(getEventBootsrapServers());
        verifierInstances.add(verifier);
        verifier.setup();
        verifier.start();
    }

    protected void startWorker() {
        WorkerApp worker = new WorkerApp(getEventBootsrapServers());
        workerInstances.add(worker);
        worker.setup();
        worker.start();
    }

    @SuppressWarnings("MustBeClosedChecker")
    protected void startView() {
        logViewAdapter = LogViewAdapterStub.create(getViewBootstrapServers());
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
