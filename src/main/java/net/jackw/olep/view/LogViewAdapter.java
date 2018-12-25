package net.jackw.olep.view;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.ModificationMessage;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.PaymentModification;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class LogViewAdapter implements AutoCloseable {
    private final ViewWriteAdapter viewAdapter;
    private final Consumer<Integer, ModificationMessage> logConsumer;

    public LogViewAdapter(Consumer<Integer, ModificationMessage> logConsumer, ViewWriteAdapter viewAdapter) {
        this.viewAdapter = viewAdapter;
        this.logConsumer = logConsumer;
    }

    /**
     * Run the adapter, sending modification events to the view
     *
     * This method blocks this thread until it's interrupted
     */
    public void run() {
        while (true) {
            try {
                ConsumerRecords<Integer, ModificationMessage> records = logConsumer.poll(Duration.ofHours(6));
                for (ConsumerRecord<Integer, ModificationMessage> record : records) {
                    processModification(record.key(), record.value());
                }
            } catch (WakeupException e) {
                break;
            }
        }
    }

    private void processModification(int key, ModificationMessage message) {
        if (message instanceof NewOrderModification) {
            viewAdapter.newOrder((NewOrderModification) message);
        } else if (message instanceof DeliveryModification) {
            viewAdapter.delivery((DeliveryModification) message);
        } else if (message instanceof PaymentModification) {
            viewAdapter.payment((PaymentModification) message);
        } else {
            throw new IllegalArgumentException("Unrecognised message type " + message.getClass().getName());
        }
    }

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public static LogViewAdapter init(String bootstrapServers, String viewServer) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "view-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<Integer, ModificationMessage> consumer = null;
        ViewWriteAdapter viewAdapter = null;

        try {
            consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(List.of(KafkaConfig.MODIFICATION_LOG));

            viewAdapter = new RedisAdapter(viewServer);

            return new LogViewAdapter(consumer, viewAdapter);
        } catch (Exception e) {
            try (
                Consumer c = consumer;
                ViewWriteAdapter v = viewAdapter;
            ) { }

            throw e;
        }
    }

    public static void main(String[] args) {
        try (LogViewAdapter adapter = init("localhost:9092", "localhost")) {
            adapter.run();
        }
    }

    @Override
    public void close() {
        // Use try-with-resources to ensure they both get safely closed
        try (
            Consumer c = logConsumer;
            ViewWriteAdapter v = viewAdapter
        ) { }
    }
}
