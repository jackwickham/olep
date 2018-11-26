package net.jackw.olep;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Abstract application for stream processors
 */
public abstract class StreamsApp {
    /**
     * A latch to catch when the application is shutting down
     */
    private final CountDownLatch appShutdownLatch;

    /**
     * The streams used by this stream processor
     */
    private KafkaStreams streams;

    /**
     * The bootstrap servers
     */
    private String bootstrapServers;

    protected StreamsApp(String bootstrapServers) {
        appShutdownLatch = new CountDownLatch(1);
        this.bootstrapServers = bootstrapServers;
    }

    protected KafkaStreams getStreams() {
        // Set up the properties of this application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        Topology topology = getTopology();
        System.out.println(topology.describe());

        return new KafkaStreams(topology, props);
    }

    /**
     * By default, do nothing
     *
     * Classes can override this to perform some initial setup, such as consuming from immutable logs
     */
    protected void setup() { }

    /**
     * Get the stream processor topology
     */
    protected abstract Topology getTopology();

    /**
     * Get this processor's application ID
     */
    public abstract String getApplicationID();

    /**
     * Run the Kafka application
     */
    public void run() {
        // Set up the streams
        setup();
        streams = getStreams();

        // Add a shutdown listener to gracefully handle Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                appShutdownLatch.countDown();
            }
        });

        // Run forever
        try {
            streams.start();
            appShutdownLatch.await();
            System.out.println("Shutting down");
            shutdown();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @OverridingMethodsMustInvokeSuper
    protected void shutdown() throws InterruptedException {
        streams.close();
    }

    protected String getBootstrapServers() {
        return bootstrapServers;
    }
}
