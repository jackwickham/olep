package net.jackw.olep.integration;

import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.transaction_worker.WorkerApp;
import net.jackw.olep.utils.ClusterCreator;
import net.jackw.olep.utils.immutable_stores.PopulateStores;
import net.jackw.olep.verifier.VerifierApp;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.After;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class BaseIntegrationTest {
    @BeforeClass
    public static void resetTopics() throws InterruptedException, ExecutionException {
        ClusterCreator.create(false);

        PopulateStores storePopulator = new PopulateStores(10, 10, 5, 20, 5);
        storePopulator.populate();
    }

    protected String getEventBoostrapServers() {
        return "127.0.0.1:9092";
    }

    protected String getViewBootstrapServers() {
        return "127.0.0.1";
    }

    private List<KafkaStreams> verifierStreams = new ArrayList<>();
    private List<KafkaStreams> workerStreams = new ArrayList<>();

    protected void startVerifier() {
        VerifierApp verifier = new VerifierApp(getEventBoostrapServers());
        verifierStreams.add(startStreamsApp(verifier));
    }

    protected void startWorker() {
        WorkerApp worker = new WorkerApp(getEventBoostrapServers());
        workerStreams.add(startStreamsApp(worker));
    }

    private KafkaStreams startStreamsApp(StreamsApp app) {
        app.setup();
        KafkaStreams streams = app.getStreams();
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
