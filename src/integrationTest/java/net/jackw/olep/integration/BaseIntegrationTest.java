package net.jackw.olep.integration;

import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.worker.WorkerApp;
import net.jackw.olep.utils.Resetter;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.StandaloneRegistry;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class BaseIntegrationTest {
    /**
     * Set up the immutable stuff once per class (JUnit doesn't really allow once per suite)
     */
    @BeforeClass
    public static void resetImmutableTopics() throws InterruptedException, ExecutionException {
        new Resetter(true, false, 10, 10, 5,
            20, getCustomerNameRange(), true).reset();
    }

    @Before
    public void resetMutableTopics() throws InterruptedException, ExecutionException {
        new Resetter(false, true, 10, 10, 5,
            20, getCustomerNameRange(), true).reset();
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

    private List<KafkaStreams> verifierStreams = new ArrayList<>();
    private List<KafkaStreams> workerStreams = new ArrayList<>();
    private LogViewAdapterStub logViewAdapter = null;

    /**
     * Start a verifier instance, with a fresh state store
     */
    protected void startVerifier() {
        VerifierApp verifier = new VerifierApp(getEventBootsrapServers());
        verifierStreams.add(startStreamsApp(verifier));
    }

    protected void startWorker() {
        WorkerApp worker = new WorkerApp(getEventBootsrapServers());
        workerStreams.add(startStreamsApp(worker));
    }

    private KafkaStreams startStreamsApp(StreamsApp app) {
        app.setup();
        KafkaStreams streams = app.getStreams();
        streams.start();
        return streams;
    }

    @SuppressWarnings("MustBeClosedChecker")
    protected void startView() {
        logViewAdapter = LogViewAdapterStub.create(getViewBootstrapServers());
    }

    @After
    public void stopWorkers() {
        for (KafkaStreams streams : verifierStreams) {
            streams.close();
            streams.cleanUp();
        }
        for (KafkaStreams streams : workerStreams) {
            streams.close();
            streams.cleanUp();
        }
        if (logViewAdapter != null) {
            logViewAdapter.close();
        }
    }
}
