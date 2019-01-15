package net.jackw.olep.integration;

import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.transaction_worker.WorkerApp;
import net.jackw.olep.utils.ClusterCreator;
import net.jackw.olep.utils.immutable_stores.PopulateStores;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.LogViewAdapter;
import net.jackw.olep.view.StandaloneRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
    private Thread viewThread = null;

    private volatile RuntimeException remoteThreadException = null;

    /**
     * Start a verifier instance, with a fresh state store
     */
    protected void startVerifier() {
        VerifierApp verifier = new VerifierApp(getEventBootsrapServers());
        verifierStreams.add(startStreamsApp(verifier, verifierStreams.isEmpty()));
    }

    protected void startWorker() {
        WorkerApp worker = new WorkerApp(getEventBootsrapServers());
        workerStreams.add(startStreamsApp(worker, workerStreams.isEmpty()));
    }

    private KafkaStreams startStreamsApp(StreamsApp app, boolean cleanup) {
        app.setup();
        KafkaStreams streams = app.getStreams();
        if (cleanup) {
            // Remove any existing state
            streams.cleanUp();

            // Then delete all of those topics, to remove all the items and state about them
            Properties adminClientConfig = new Properties();
            adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            try (AdminClient adminClient = AdminClient.create(adminClientConfig)) {
                Set<String> existingTopics = adminClient.listTopics().names().get();
                existingTopics.retainAll(List.of(
                    "worker-customer-mutable-changelog",
                    "worker-district-next-order-id-changelog",
                    "worker-new-orders-changelog",
                    "worker-stock-quantity-changelog"
                ));
                DeleteTopicsResult deleteResult = adminClient.deleteTopics(existingTopics);

                // Wait for the deletion to complete, so we don't have problems when creating
                deleteResult.all().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        streams.start();
        return streams;
    }

    protected void startView() {
        final CountDownLatch latch = new CountDownLatch(1);
        viewThread = new Thread(() -> {
            try {
                try (LogViewAdapter adapter = LogViewAdapter.init(
                    getEventBootsrapServers(),
                    getViewBootstrapServers()
                )) {
                    latch.countDown();
                    adapter.run();
                }
            } catch (InterruptedException | InterruptException e) {
                // pass
            } catch (Exception e) {
                remoteThreadException = new RuntimeException(e);
            }
            if (latch.getCount() > 0) {
                latch.countDown();
            }
        });
        viewThread.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void stopWorkers() throws InterruptedException {
        for (KafkaStreams streams : verifierStreams) {
            streams.close();
        }
        for (KafkaStreams streams : workerStreams) {
            streams.close();
        }
        if (viewThread != null && viewThread.isAlive()) {
            viewThread.interrupt();
            viewThread.join();
        }

        if (remoteThreadException != null) {
            throw remoteThreadException;
        }
    }
}
