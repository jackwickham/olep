package net.jackw.olep.common;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
     * Get the future that should resolve before the Kafka Streams application should start
     */
    public ListenableFuture<?> getBeforeStartFuture() {
        return Futures.immediateFuture(null);
    }

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

    public void start() throws InterruptedException, ExecutionException {
        streams = getStreams();
        streams.cleanUp();

        getBeforeStartFuture().get();
        log.info("Starting Kafka streams");
        streams.start();
    }

    /**
     * Run the Kafka application
     */
    public void run(Runnable readyCallback) {
        boolean error = false;

        try {
            // Add a shutdown listener to gracefully handle Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    appShutdownLatch.countDown();
                }
            });

            start();

            // Send the notification that kafka is now running
            readyCallback.run();

            // Run forever
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

    public void run() {
        this.run(() -> {});
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

    public static void createReadyFile(String readyFile) {
        if (readyFile != null && !readyFile.isBlank()) {
            try {
                File file = new File(readyFile);
                boolean created = file.createNewFile();
                if (created) {
                    file.deleteOnExit();
                } else {
                    log.warn("Failed to create ready file: file already exists");
                }
            } catch (IOException e) {
                log.warn("Failed to create ready file", e);
            }
        }
    }

    private static Logger log = LogManager.getLogger();
}
