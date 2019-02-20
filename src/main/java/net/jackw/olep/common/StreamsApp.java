package net.jackw.olep.common;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import com.google.errorprone.annotations.ForOverride;
import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract application for stream processors
 */
public abstract class StreamsApp implements AutoCloseable {
    /**
     * A latch to catch when the application is shutting down
     */
    private final CountDownLatch appShutdownLatch;

    /**
     * The streams used by this stream processor
     */
    private KafkaStreams streams;

    /**
     * The database config
     */
    private DatabaseConfig config;

    protected StreamsApp(DatabaseConfig config) {
        appShutdownLatch = new CountDownLatch(1);
        this.config = config;
    }

    public KafkaStreams getStreams() {
        // Set up the properties of this application
        Properties props = getStreamProperties();

        Topology topology = getTopology();
        log.debug(topology.describe());

        return new KafkaStreams(topology, props);
    }

    /**
     * Wait for the app to perform all of the necessary setup
     *
     * This method is called immediately before the Kafka streams app is started, and is expected to block
     */
    @ForOverride
    protected void beforeStart() throws Exception { }

    /**
     * Get the stream processor topology
     */
    protected abstract Topology getTopology();

    /**
     * Get this processor's application ID
     */
    public abstract String getApplicationID();

    /**
     * Get the number of threads that should be created for this application
     */
    protected abstract int getThreadCount();

    /**
     * Get the properties that should be set on the Kafka Streams app
     */
    protected Properties getStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, config.getStreamsStateDir());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, getThreadCount());
        return props;
    }

    /**
     * Cache for the generated node ID
     */
    private Integer nodeId = null;

    /**
     * Get a unique identifier for this node
     */
    public final int getNodeID() {
        if (nodeId == null) {
            Random rand = new Random();
            nodeId = rand.nextInt();
        }
        return nodeId;
    }

    public void start() throws Exception {
        streams = getStreams();
        streams.cleanUp();

        beforeStart();
        streams.start();
    }

    /**
     * Run the Kafka application
     */
    public void run() {
        boolean error = false;

        try {
            // Add a shutdown listener to gracefully handle Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    appShutdownLatch.countDown();
                }
            });

            // Run forever
            start();
            appShutdownLatch.await();
            log.info("Shutting down");
        } catch (Throwable e) {
            log.fatal(e);
            error = true;
        } finally {
            try {
                close();
            } catch (InterruptedException e) {
                // Nothing we can do now
                log.fatal("Uncaught exception while shutting down", e);
                error = true;
            }
        }

        if (error) {
            System.exit(1);
        }
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public void close() throws InterruptedException {
        if (streams != null) {
            streams.close();
        }
    }

    public void cleanup() {
        getStreams().cleanUp();
    }

    protected String getBootstrapServers() {
        return config.getBootstrapServers();
    }

    private static Logger log = LogManager.getLogger();
}
