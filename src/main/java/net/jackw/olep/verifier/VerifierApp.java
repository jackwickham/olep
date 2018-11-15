package net.jackw.olep.verifier;

import com.google.common.collect.ImmutableMap;
import net.jackw.olep.StreamsApp;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.message.TransactionRequestMessage;
import net.jackw.olep.message.TransactionResultMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Random;

public class VerifierApp extends StreamsApp {
    @Override
    public String getApplicationID() {
        return "verifier";
    }

    @Override
    protected Topology getTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Ingest messages from the transaction request topic
        KStream<String, TransactionRequestMessage> source = builder.stream(
            KafkaConfig.TRANSACTION_REQUEST_TOPIC,
            Consumed.with(Serdes.String(), new JsonSerde<>(TransactionRequestMessage.class))
        );
        // Approve them
        source
            .mapValues(value -> new TransactionResultMessage(value.transactionId, true, null))
            .to(
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                Produced.with(Serdes.String(), new JsonSerde<>(TransactionResultMessage.class))
            );

        // Add results
        source
            .mapValues(value -> {
                Random rand = new Random();
                ImmutableMap<String, Object> m = ImmutableMap.of("rnd", rand.nextInt(), "hello", "world");
                return new TransactionResultMessage(value.transactionId, m);
            })
            .to(
                KafkaConfig.TRANSACTION_RESULT_TOPIC,
                Produced.with(Serdes.String(), new JsonSerde<>(TransactionResultMessage.class))
            );

        return builder.build();
    }

    public static void main(String[] args) {
        StreamsApp instance = new VerifierApp();
        instance.run();
    }
}
