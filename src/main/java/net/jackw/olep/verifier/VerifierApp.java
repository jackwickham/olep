package net.jackw.olep.verifier;

import net.jackw.olep.StreamsApp;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;

public class VerifierApp extends StreamsApp {
    @Override
    public String getApplicationID() {
        return "streams-wordcount";
    }

    @Override
    protected Topology getTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        // Take plaintext in
        KStream<String, String> source = builder.stream("streams-plaintext-input");
        // And split them into lines
        source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count(Materialized.as("counts-store"))
            .toStream()
            .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        StreamsApp instance = new VerifierApp();
        instance.run();
    }
}
