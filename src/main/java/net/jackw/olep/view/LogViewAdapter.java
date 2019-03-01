package net.jackw.olep.view;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.LockingLRUSet;
import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.common.store.SharedCustomerStoreConsumer;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.ModificationKey;
import net.jackw.olep.message.modification.ModificationMessage;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LogViewAdapter extends Thread implements AutoCloseable, ConsumerRebalanceListener {
    private final ViewWriteAdapter viewAdapter;
    private final Consumer<ModificationKey, ModificationMessage> logConsumer;
    private final SharedCustomerStoreConsumer customerStoreConsumer;
    private final Metrics metrics;
    private final SettableFuture<Void> readyFuture;
    private final Set<ModificationKey> recentTransactions;
    private final Set<Integer> readyPartitions;
    private final Map<Integer, Long> endOffsets;
    private final AtomicInteger assignedPartitionCount = new AtomicInteger(0);
    private final Thread partitionEndOffsetUpdateThread;
    private final ExecutorService executorService;

    public LogViewAdapter(Consumer<ModificationKey, ModificationMessage> logConsumer, ViewWriteAdapter viewAdapter,
                          SharedCustomerStoreConsumer customerStoreConsumer, Metrics metrics) {
        super("log-view-adapter");

        this.logConsumer = logConsumer;
        this.viewAdapter = viewAdapter;
        this.customerStoreConsumer = customerStoreConsumer;
        this.metrics = metrics;
        this.readyFuture = SettableFuture.create();
        this.recentTransactions = new LockingLRUSet<>(100);

        this.readyPartitions = Collections.synchronizedSet(new HashSet<>());
        this.endOffsets = new ConcurrentHashMap<>();

        this.executorService = Executors.newCachedThreadPool();

        logConsumer.subscribe(List.of(KafkaConfig.MODIFICATION_LOG), this);

        partitionEndOffsetUpdateThread = new Thread(() -> {
            try {
                while (true) {
                    Thread.sleep(5000);
                    updateEndOffsets();
                }
            } catch (InterruptedException e) {
                // Pass
            }
        });
        partitionEndOffsetUpdateThread.start();
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
        while (true) {
            // ! Critical path ! \\
            try {
                ConsumerRecords<ModificationKey, ModificationMessage> records = logConsumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<ModificationKey, ModificationMessage> record : records) {
                    processModification(record.key(), record.value());
                    int partition = record.partition();

                    if (!readyPartitions.contains(partition)) {
                        Long endOffset = endOffsets.get(partition);

                        if (endOffset != null && record.offset() >= endOffset - 1) {
                            // We're caught up on this partition
                            markReady(partition);
                        }
                    }
                }
            } catch (WakeupException e) {
                break;
            }
        }
    }

    /**
     * Update the view with a modification
     *
     * @param key The modification key
     * @param message The modification body
     */
    private void processModification(ModificationKey key, ModificationMessage message) {
        // ! Critical path ! \\
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

    /**
     * Check whether all of the partitions are now registered in the RMI registry, and set the readyFuture if so
     */
    private void checkReadiness() {
        if (assignedPartitionCount.get() == readyPartitions.size()) {
            // We're ready, so let listeners know
            readyFuture.set(null);
        }
    }

    /**
     * Mark the partition as ready, in a separate thread to move network requests away from the critical path
     *
     * @param partition The partition that has become ready
     */
    private void markReady(int partition) {
        executorService.execute(() -> {
            if (!viewAdapter.register(partition)) {
                // :(
                try {
                    close();
                } catch (InterruptedException e) {
                    log.error("Error while closing");
                }
            } else {
                // successfully registered, so note it and check whether the others are already done
                readyPartitions.add(partition);
                checkReadiness();
            }
        });
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

    @Override
    public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        int actuallyRemovedPartitions = 0;
        for (TopicPartition topicPartition : partitions) {
            readyPartitions.remove(topicPartition.partition());
            if (endOffsets.remove(topicPartition.partition()) != null) {
                ++actuallyRemovedPartitions;
            }
            viewAdapter.unregister(topicPartition.partition());
        }
        assignedPartitionCount.addAndGet(-actuallyRemovedPartitions);
        checkReadiness();
    }

    @Override
    public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assignedPartitionCount.addAndGet(partitions.size());
        int notReassigned = 0;
        for (Map.Entry<TopicPartition, Long> entry : logConsumer.endOffsets(partitions).entrySet()) {
            if (endOffsets.put(entry.getKey().partition(), entry.getValue()) != null) {
                // We were already subscribed to this partition
                ++notReassigned;
            }
            if (logConsumer.position(entry.getKey()) == entry.getValue()) {
                // We're already caught up on this partition
                readyPartitions.add(entry.getKey().partition());
            }
            // Don't register with the views here
        }
        // Correct for any that were still subscribed
        assignedPartitionCount.addAndGet(-notReassigned);
        checkReadiness();
    }

    /**
     * Get the end offsets for the partitions assigned to this adapter
     */
    private synchronized void updateEndOffsets() {
        Collection<TopicPartition> assignedPartitions = endOffsets.keySet()
            .stream()
            .map(p -> new TopicPartition(KafkaConfig.MODIFICATION_LOG, p))
            .collect(Collectors.toList());
        endOffsets.putAll(logConsumer.endOffsets(assignedPartitions)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> e.getKey().partition(),
                Map.Entry::getValue
            )));
    }

    @SuppressWarnings("MustBeClosedChecker")
    public static LogViewAdapter init(DatabaseConfig config) throws RemoteException, InterruptedException {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "view-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // We don't care about being reset back to here on restart
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Prefer not to change partition assignment
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());

        KafkaConsumer<ModificationKey, ModificationMessage> consumer = null;
        SharedCustomerStoreConsumer customerStoreConsumer = null;

        try {
            consumer = new KafkaConsumer<>(
                consumerProps,
                new ModificationKey.KeyDeserializer(),
                new JsonDeserializer<>(ModificationMessage.class)
            );

            customerStoreConsumer = SharedCustomerStoreConsumer.create(config.getBootstrapServers(), "view-adapter-" + System.nanoTime(), config);

            ViewWriteAdapter viewWriteAdapter = new InMemoryAdapter(customerStoreConsumer.getStore(), config);

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
        try (LogViewAdapter adapter = init(arguments.getConfig())) {
            adapter.start();

            // Add a shutdown listener to gracefully handle Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread("view-adapter-shutdown-hook") {
                @Override
                public void run() {
                    try {
                        adapter.close();
                    } catch (Exception e) {
                        log.error(e);
                    }
                }
            });

            // Wait for the adapter to be ready
            adapter.getReadyFuture().get();
            StreamsApp.createReadyFile(arguments.getReadyFileArg());

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
            partitionEndOffsetUpdateThread.interrupt();
            logConsumer.wakeup();
            readyFuture.setException(new CancellationException());
            if (Thread.currentThread() != this) {
                this.join();
            }
            partitionEndOffsetUpdateThread.join();
        }
    }

    private static Logger log = LogManager.getLogger();
}
