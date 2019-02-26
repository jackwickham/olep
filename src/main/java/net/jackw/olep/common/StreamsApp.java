package net.jackw.olep.common;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.MustBeClosed;
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
     * The streams used by this stream processor
     */
    private KafkaStreams streams;

    /**
     * The database config
     */
    private DatabaseConfig config;

    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    @MustBeClosed
    protected StreamsApp(DatabaseConfig config) {
        this.config = config;
    }

    protected KafkaStreams createStreams() {
        // Set up the properties of this application
        Properties props = getStreamProperties();

        Topology topology = getTopology();
        log.debug(topology.describe());

        return new KafkaStreams(topology, props);
    }

    /**
     * Get the future that should resolve before the Kafka Streams application should start
     */
    protected ListenableFuture<?> getBeforeStartFuture() {
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

    /**
     * Start the process
     *
     * This call will block until the immutable stores are fully populated (they started when the class was created)
     */
    public void start() throws InterruptedException, ExecutionException {
        streams = createStreams();
        streams.cleanUp();

        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                log.fatal("Kafka streams have transitioned to error state");
                try {
                    close();
                } catch (InterruptedException e) {
                    log.error("Interrupted exception while closing from error state");
                }
            }
        });

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            try {
                log.fatal("Uncaught exception in kafka stream thread", throwable);
                close();
            } catch (InterruptedException e) {
                log.error("Interrupted exception while closing from uncaught exception in thread");
            }
        });

        getBeforeStartFuture().get();
        log.info("Starting Kafka streams");
        streams.start();
    }

    /**
     * Start the process, then block until it shuts down
     *
     * @param readyFile The file to write a ready state to, or null
     */
    public void runForever(String readyFile) throws InterruptedException, ExecutionException {
        start();

        // Once we get here, we are ready (or will be very soon)
        StreamsApp.createReadyFile(readyFile);

        // Add a shutdown listener to gracefully handle Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                try {
                    close();
                } catch (InterruptedException e) {
                    log.error("Interrupted while trying to close app", e);
                }
            }
        });

        // Just wait for Ctrl+C (block forever)
        shutdownLatch.await();
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public void close() throws InterruptedException {
        if (streams != null) {
            streams.close();
            streams = null;
            shutdownLatch.countDown();
        }
    }

    public void cleanup() {
        createStreams().cleanUp();
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

    @VisibleForTesting
    public KafkaStreams getStreams() {
        return streams;
    }

    private static Logger log = LogManager.getLogger();
}
