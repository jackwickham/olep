package net.jackw.olep.common;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;
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

    /**
     * Latch that will go to zero after the streams app shuts down
     */
    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    /**
     * Latch that is closed when the streams app is running, and open when it's rebalancing or otherwise not running
     */
    private Latch streamsRunningLatch = new Latch(false);

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
     * Get the number of state stores that this processor has
     */
    protected abstract int getStateStoreCount();

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
     * Get a latch that will be closed when the streams app is in state RUNNING, and open otherwise
     */
    public Latch getStreamsRunningLatch() {
        return streamsRunningLatch;
    }

    /**
     * Start the process
     *
     * This call will block until the immutable stores are fully populated (they started when the class was created)
     */
    public void start() throws InterruptedException, ExecutionException {
        streams = createStreams();
        streams.cleanUp();

        CountUpDownLatch readyLatch = new CountUpDownLatch(getStateStoreCount() * config.getAcceptedTransactionTopicPartitions() + 1);

        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                log.fatal("Kafka streams have transitioned to error state");
                try {
                    close();
                } catch (InterruptedException e) {
                    log.error("Interrupted exception while closing from error state");
                }
            } else if (newState == KafkaStreams.State.RUNNING) {
                streamsRunningLatch.close();
                readyLatch.countDown();
            } else if (oldState == KafkaStreams.State.RUNNING) {
                streamsRunningLatch.open();
                readyLatch.countUp();
            }
        });

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            log.fatal("Uncaught exception in kafka stream thread", throwable);
            // Run close in a new thread to prevent deadlock
            new Thread(() -> {
                try {
                    close();
                } catch (InterruptedException e) {
                    log.error("Interrupted exception while closing from uncaught exception in thread");
                }
            }).start();
        });

        Multimap<String, Integer> initialisedMutableStores = Multimaps.synchronizedSetMultimap(
            HashMultimap.create(getStateStoreCount(), config.getAcceptedTransactionTopicPartitions())
        );

        streams.setGlobalStateRestoreListener(new StateRestoreListener() {
            @Override
            public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) { }

            @Override
            public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) { }

            @Override
            public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
                if (initialisedMutableStores.put(topicPartition.topic(), topicPartition.partition())) {
                    // This store became ready for the first time
                    readyLatch.countDown();
                }
            }
        });

        getBeforeStartFuture().get();
        log.info("Starting Kafka streams");
        streams.start();

        readyLatch.await();

        log.info("Mutable state stores populated");
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

    /**
     * Block the current thread until the streams app shuts down
     */
    public void awaitShutdown() throws InterruptedException {
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
        KafkaStreams streams = createStreams();
        streams.cleanUp();
        streams.close();
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
