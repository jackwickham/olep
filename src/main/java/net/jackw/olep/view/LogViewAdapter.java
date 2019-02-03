package net.jackw.olep.view;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.store.SharedCustomerStoreConsumer;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.ModificationMessage;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class LogViewAdapter extends Thread implements AutoCloseable {
    private final InMemoryRMIWrapper viewWrapper;
    private final ViewWriteAdapter viewAdapter;
    private final Consumer<Long, ModificationMessage> logConsumer;

    public LogViewAdapter(Consumer<Long, ModificationMessage> logConsumer, InMemoryRMIWrapper viewWrapper) {
        this.viewWrapper = viewWrapper;
        this.viewAdapter = viewWrapper.getAdapter();
        this.logConsumer = logConsumer;

        // Add a shutdown listener to gracefully handle Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread("view-adapter-shutdown-hook") {
            @Override
            public void run() {
                try {
                    close();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        });
    }

    /**
     * Run the adapter, sending modification events to the view
     *
     * This method blocks this thread until it's interrupted
     */
    @Override
    public void run() {
        while (true) {
            try {
                ConsumerRecords<Long, ModificationMessage> records = logConsumer.poll(Duration.ofHours(6));
                for (ConsumerRecord<Long, ModificationMessage> record : records) {
                    processModification(record.key(), record.value());
                }
            } catch (WakeupException e) {
                break;
            }
        }
    }

    private void processModification(long key, ModificationMessage message) {
        log.debug("Processing {} for transaction {}", message.getClass(), key);
        if (message instanceof NewOrderModification) {
            viewAdapter.newOrder((NewOrderModification) message);
        } else if (message instanceof DeliveryModification) {
            viewAdapter.delivery((DeliveryModification) message);
        } else if (message instanceof PaymentModification) {
            viewAdapter.payment((PaymentModification) message);
        } else if (message instanceof RemoteStockModification) {
            viewAdapter.remoteStock((RemoteStockModification) message);
        } else {
            throw new IllegalArgumentException("Unrecognised message type " + message.getClass().getName());
        }
    }

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public static LogViewAdapter init(String bootstrapServers, String registryServer) throws RemoteException, AlreadyBoundException, NotBoundException, InterruptedException {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "view-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Long, ModificationMessage> consumer = null;
        InMemoryRMIWrapper viewWrapper = null;
        SharedCustomerStoreConsumer customerStoreConsumer = null;

        try {
            consumer = new KafkaConsumer<>(
                consumerProps,
                Serdes.Long().deserializer(),
                new JsonDeserializer<>(ModificationMessage.class)
            );
            // TODO: support partitioning
            List<TopicPartition> partitions = List.of(new TopicPartition(KafkaConfig.MODIFICATION_LOG, 0));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            customerStoreConsumer = SharedCustomerStoreConsumer.create(bootstrapServers, "view-adapter-TODO_PARTITION_ID-" + new Date().getTime());

            viewWrapper = new InMemoryRMIWrapper(registryServer, customerStoreConsumer.getStore());

            return new LogViewAdapter(consumer, viewWrapper);
        } catch (Exception e) {
            try (
                Consumer c = consumer;
                InMemoryRMIWrapper v = viewWrapper;
                SharedCustomerStoreConsumer scs = customerStoreConsumer;
            ) { }

            throw e;
        }
    }

    public static void main(String[] args) throws RemoteException, AlreadyBoundException, NotBoundException, InterruptedException {
        try (LogViewAdapter adapter = init("localhost:9092", "localhost")) {
            adapter.start();
            adapter.join();
        }
    }

    @Override
    public void close() throws RemoteException, NotBoundException {
        // Use try-with-resources to ensure they all get safely closed
        try (
            Consumer c = logConsumer;
            InMemoryRMIWrapper v = viewWrapper;
        ) {
            logConsumer.wakeup();
        }
    }

    private static Logger log = LogManager.getLogger();
}
