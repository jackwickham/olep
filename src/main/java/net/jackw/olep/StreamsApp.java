package net.jackw.olep;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public abstract class StreamsApp {
    private final CountDownLatch appShutdownLatch;
    private KafkaStreams streams;

    protected StreamsApp() {
        appShutdownLatch = new CountDownLatch(1);
    }

    protected KafkaStreams getStreams() {
        // Set up the properties of this application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Serdes for the keys and values if not otherwise specified (to be removed later I think)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final Topology topology = getTopology();
        System.out.println(topology.describe());

        return new KafkaStreams(topology, props);
    }

    protected abstract Topology getTopology();

    public abstract String getApplicationID();

    /**
     * Run the Kafka application
     */
    public void run() {
        // Set up the streams
        streams = getStreams();

        // Add a shutdown listener to gracefully handle Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                appShutdownLatch.countDown();
            }
        });

        // Run forever
        try {
            streams.start();
            appShutdownLatch.await();
            System.out.println("Shutting down");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
