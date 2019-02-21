package net.jackw.olep.view;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.LockingLRUSet;
import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.common.store.SharedCustomerStoreConsumer;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.ModificationMessage;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
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

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

public class LogViewAdapter extends Thread implements AutoCloseable {
    private final ViewWriteAdapter viewAdapter;
    private final Consumer<Long, ModificationMessage> logConsumer;
    private final SharedCustomerStoreConsumer customerStoreConsumer;
    private final Metrics metrics;
    private final SettableFuture<Void> readyFuture;
    private final Set<Long> recentTransactions;

    public LogViewAdapter(Consumer<Long, ModificationMessage> logConsumer, ViewWriteAdapter viewAdapter,
                          SharedCustomerStoreConsumer customerStoreConsumer, Metrics metrics) {
        this.logConsumer = logConsumer;
        this.viewAdapter = viewAdapter;
        this.customerStoreConsumer = customerStoreConsumer;
        this.metrics = metrics;
        this.readyFuture = SettableFuture.create();
        this.recentTransactions = new LockingLRUSet<>(100);

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
        // Wait for the customer store to populate
        try {
            customerStoreConsumer.getReadyFuture().get();
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed while waiting for customer store to be populated", e);
            return;
        }

        // Then populate the views from the modification log
        boolean ready = false;
        Map<TopicPartition, Long> endOffsets = logConsumer.endOffsets(logConsumer.assignment());
        while (true) {
            try {
                ConsumerRecords<Long, ModificationMessage> records = logConsumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<Long, ModificationMessage> record : records) {
                    processModification(record.key(), record.value());

                    if (!ready && record.offset() >= endOffsets.get(new TopicPartition(record.topic(), record.partition())) - 5) {
                        // Simple approximation, it's ready now
                        ready = true;

                        if (!viewAdapter.register()) {
                            try {
                                close();
                            } catch (InterruptedException e) {
                                log.error("Error when closing", e);
                            }
                            return;
                        }
                        // Only set ready if it was successfully registered
                        readyFuture.set(null);
                    }
                }
            } catch (WakeupException e) {
                break;
            }
        }
    }

    /**
     * Get a future that resolves when the adapter has almost caught up to the latest items on the modification log
     *
     * The future just means that it had caught up at one point - if it becomes out of sync later, the future will not
     * change state
     */
    public ListenableFuture<Void> getReadyFuture() {
        return readyFuture;
    }

    private void processModification(long key, ModificationMessage message) {
        if (!recentTransactions.add(key)) {
            log.info("Received duplicate transaction {}", key);
            return;
        }
        Timer timer = metrics.startTimer();
        log.debug("Processing {} for transaction {}", message.getClass(), key);
        if (message instanceof NewOrderModification) {
            viewAdapter.newOrder((NewOrderModification) message);
            metrics.recordDuration(DurationType.VIEW_NEW_ORDER, timer);
        } else if (message instanceof DeliveryModification) {
            viewAdapter.delivery((DeliveryModification) message);
            metrics.recordDuration(DurationType.VIEW_DELIVERY, timer);
        } else if (message instanceof PaymentModification) {
            viewAdapter.payment((PaymentModification) message);
            metrics.recordDuration(DurationType.VIEW_PAYMENT, timer);
        } else if (message instanceof RemoteStockModification) {
            viewAdapter.remoteStock((RemoteStockModification) message);
            metrics.recordDuration(DurationType.VIEW_NEW_ORDER_REMOTE_STOCK, timer);
        } else {
            throw new IllegalArgumentException("Unrecognised message type " + message.getClass().getName());
        }
    }

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public static LogViewAdapter init(String bootstrapServers, String registryServer, DatabaseConfig config) throws RemoteException, InterruptedException {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "view-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Long, ModificationMessage> consumer = null;
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

            customerStoreConsumer = SharedCustomerStoreConsumer.create(bootstrapServers, "view-adapter-TODO_PARTITION_ID-" + new Date().getTime(), config);

            ViewWriteAdapter viewWriteAdapter = new InMemoryAdapter(customerStoreConsumer.getStore(), registryServer, "view/TODO_PARTITION_NUMBER");

            return new LogViewAdapter(consumer, viewWriteAdapter, customerStoreConsumer, config.getMetrics());
        } catch (Exception e) {
            try (
                Consumer c = consumer;
                SharedCustomerStoreConsumer scs = customerStoreConsumer;
            ) { }

            throw e;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        Arguments arguments = new Arguments(args);
        try (LogViewAdapter adapter = init(
            arguments.getConfig().getBootstrapServers(), arguments.getConfig().getViewRegistryHost(), arguments.getConfig()
        )) {
            adapter.getReadyFuture().get();
            // Notify other processes if needed
            StreamsApp.createReadyFile(arguments.getReadyFileArg());

            adapter.start();
            adapter.join();
        }
    }

    @Override
    public void close() throws InterruptedException {
        // Use try-with-resources to ensure they all get safely closed
        try (
            Consumer c = logConsumer;
            ViewWriteAdapter va = viewAdapter;
            SharedCustomerStoreConsumer csc = customerStoreConsumer;
        ) {
            logConsumer.wakeup();
            readyFuture.setException(new CancellationException());
        }
    }

    private static Logger log = LogManager.getLogger();
}
