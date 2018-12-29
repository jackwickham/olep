package net.jackw.olep.integration;

import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.transaction_worker.WorkerApp;
import net.jackw.olep.utils.ClusterCreator;
import net.jackw.olep.utils.immutable_stores.PopulateStores;
import net.jackw.olep.verifier.VerifierApp;
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
    public static void resetTopics() throws InterruptedException, ExecutionException {
        ClusterCreator.resetAll();

        try (PopulateStores storePopulator = new PopulateStores(10, 10, 5, 20, getCustomerNameRange(), true)) {
            storePopulator.populate();
        }
    }

    @Before
    public void resetTransactionTopics() throws InterruptedException, ExecutionException {
        ClusterCreator.resetTransactionTopics();
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
        // Remove any existing state
        streams.cleanUp();
        streams.start();
        return streams;
    }

    @After
    public void stopWorkers() {
        for (KafkaStreams streams : verifierStreams) {
            streams.close();
        }
        for (KafkaStreams streams : workerStreams) {
            streams.close();
        }
    }
}
