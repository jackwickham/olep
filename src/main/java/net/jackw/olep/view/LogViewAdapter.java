package net.jackw.olep.view;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.InterThreadWorkQueue;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.LRUSet;
import net.jackw.olep.common.BatchingLRUSet;
import net.jackw.olep.common.Latch;
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

import java.rmi.RemoteException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class LogViewAdapter extends Thread implements AutoCloseable, ConsumerRebalanceListener {
    private final ViewWriteAdapter viewAdapter;
    private final Consumer<ModificationKey, ModificationMessage> logConsumer;
    private final SharedCustomerStoreConsumer customerStoreConsumer;
    private final Metrics metrics;
    private final SettableFuture<Void> readyFuture;
    private final LRUSet<ModificationKey> recentTransactions;
    private final Set<Integer> readyPartitions;
    private final Map<Integer, Long> endOffsets;
    private final AtomicInteger assignedPartitionCount = new AtomicInteger(0);
    private final ExecutorService executorService;

    @GuardedBy("recentlyAssignedPartitions")
    private final List<TopicPartition> recentlyAssignedPartitions;
    @GuardedBy("recentlyAssignedPartitions")
    private final List<TopicPartition> recentlyRevokedPartitions;

    private static final AtomicInteger threadId = new AtomicInteger(0);

    public LogViewAdapter(Consumer<ModificationKey, ModificationMessage> logConsumer, ViewWriteAdapter viewAdapter,
                          SharedCustomerStoreConsumer customerStoreConsumer, Metrics metrics) {
        super("log-view-adapter-" + threadId.incrementAndGet());

        this.logConsumer = logConsumer;
        this.viewAdapter = viewAdapter;
        this.customerStoreConsumer = customerStoreConsumer;
        this.metrics = metrics;
        this.readyFuture = SettableFuture.create();
        this.recentTransactions = new BatchingLRUSet<>(20000);

        this.readyPartitions = Collections.synchronizedSet(new HashSet<>());
        this.endOffsets = new ConcurrentHashMap<>();

        this.executorService = Executors.newCachedThreadPool();
        this.recentlyAssignedPartitions = new ArrayList<>(4);
        this.recentlyRevokedPartitions = new ArrayList<>(4);

        logConsumer.subscribe(List.of(KafkaConfig.MODIFICATION_LOG), this);
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
                checkNewPartitions();
                ConsumerRecords<ModificationKey, ModificationMessage> records = logConsumer.poll(Duration.ofMillis(100));
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
        try {
            // ! Critical path ! \\
            if (!recentTransactions.add(key)) {
                log.info("Received duplicate transaction {}", key);
                return;
            }
            Timer timer = metrics.startTimer();
            log.debug("Processing {} for transaction {}: {}", message.getClass(), key, message);
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
        } catch (Exception e) {
            log.fatal("Error while processing transaction #{} {}: {}", key, message, e);
            readyFuture.setException(e);
            throw e;
        }
    }

    private void checkNewPartitions() {
        List<TopicPartition> assignedPartitions;
        List<TopicPartition> revokedPartitions;
        synchronized (recentlyAssignedPartitions) {
            assignedPartitions = new ArrayList<>(recentlyAssignedPartitions);
            recentlyAssignedPartitions.clear();

            revokedPartitions = new ArrayList<>(recentlyRevokedPartitions);
            recentlyRevokedPartitions.clear();

            recentlyAssignedPartitions.notifyAll();
        }
        if (assignedPartitions.isEmpty() && revokedPartitions.isEmpty()) {
            // Nothing to do
            return;
        }
        // Load the new offsets
        Map<TopicPartition, Long> latestEndOffsets = logConsumer.endOffsets(assignedPartitions);
        List<TopicPartition> newlyAssigned = new ArrayList<>(assignedPartitions.size());
        int actuallyRemovedPartitions = 0;
        synchronized (endOffsets) {
            for (TopicPartition topicPartition : assignedPartitions) {
                if (endOffsets.put(
                    topicPartition.partition(),
                    latestEndOffsets.getOrDefault(topicPartition, 0L)
                ) == null) {
                    // Not already subscribed
                    newlyAssigned.add(topicPartition);
                }
                if (logConsumer.position(topicPartition) >= latestEndOffsets.getOrDefault(topicPartition, 0L) - 1) {
                    // We're already caught up on this partition
                    markReady(topicPartition.partition());
                }
            }

            for (TopicPartition topicPartition : revokedPartitions) {
                readyPartitions.remove(topicPartition.partition());
                if (endOffsets.remove(topicPartition.partition()) != null) {
                    ++actuallyRemovedPartitions;
                }
                // Unregister in a separate thread because it involves network communication
                executorService.execute(() -> {
                    viewAdapter.unregister(topicPartition.partition());
                });
            }
        }
        // Seek all the new partitions back to the start, because we wiped out any state when it was revoked
        logConsumer.seekToBeginning(newlyAssigned);
        // Correct the count for topics that we were already subscribed to
        assignedPartitionCount.addAndGet(newlyAssigned.size() - assignedPartitions.size() - actuallyRemovedPartitions);
        // Now see whether we're actually ready
        checkReadiness();
    }

    /**
     * Check whether all of the partitions are now registered in the RMI registry, and set the readyFuture if so
     *
     * This method requires that num partitions >= num view threads, so at least one partition is assigned to each view
     */
    private void checkReadiness() {
        int partitionCount = assignedPartitionCount.get();
        if (partitionCount > 0 && partitionCount == readyPartitions.size()) {
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
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions {} revoked", partitions);
        synchronized (recentlyAssignedPartitions) {
            recentlyAssignedPartitions.removeAll(partitions);
            recentlyRevokedPartitions.addAll(partitions);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partitions {} assigned", partitions);

        assignedPartitionCount.addAndGet(partitions.size());
        synchronized (recentlyAssignedPartitions) {
            recentlyAssignedPartitions.addAll(partitions);
            recentlyRevokedPartitions.removeAll(partitions);
            if (Thread.currentThread() == this) {
                // We can safely use Kafka, and can't wait because that would cause a deadlock waiting for itself
                checkNewPartitions();
            } else {
                try {
                    recentlyAssignedPartitions.wait();
                } catch (InterruptedException e) {
                    // Re-interrupt and continue
                    Thread.currentThread().interrupt();
                }
            }
        }
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
        // Prevent timeouts causing rebalances, which break things
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "120000");

        KafkaConsumer<ModificationKey, ModificationMessage> consumer = null;
        SharedCustomerStoreConsumer customerStoreConsumer = null;

        try {
            consumer = new KafkaConsumer<>(
                consumerProps,
                new ModificationKey.KeyDeserializer(),
                new JsonDeserializer<>(ModificationMessage.class)
            );

            customerStoreConsumer = SharedCustomerStoreConsumer.create(config.getBootstrapServers(), "view-adapter", config);

            ViewWriteAdapter viewWriteAdapter = new InMemoryView(customerStoreConsumer.getStore(), config);

            return new LogViewAdapter(consumer, viewWriteAdapter, customerStoreConsumer, config.getMetrics());
        } catch (Exception e) {
            try (
                Consumer c = consumer;
                SharedCustomerStoreConsumer scs = customerStoreConsumer;
            ) { }

            throw e;
        }
    }

    @Override
    public synchronized void close() throws InterruptedException {
        // Use try-with-resources to ensure they all get safely closed
        try (
            Consumer c = logConsumer;
            ViewWriteAdapter va = viewAdapter;
            SharedCustomerStoreConsumer csc = customerStoreConsumer;
        ) {
            readyFuture.cancel(false);
            logConsumer.wakeup();
            if (this.isAlive() && Thread.currentThread() != this) {
                this.join(50);
                if (this.isAlive()) {
                    this.interrupt();
                    this.join();
                }
            }
        }
    }

    private static Logger log = LogManager.getLogger();
}
