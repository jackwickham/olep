package net.jackw.olep.utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.utils.populate.PopulateStores;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Resetter implements AutoCloseable {
    private static final Set<String> mutableTopics = ImmutableSet.of(
        // Transaction topics
        KafkaConfig.TRANSACTION_REQUEST_TOPIC, KafkaConfig.TRANSACTION_RESULT_TOPIC,
        KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, KafkaConfig.MODIFICATION_LOG,
        // Mutable store topics
        KafkaConfig.NEW_ORDER_CHANGELOG, KafkaConfig.DISTRICT_NEXT_ORDER_ID_CHANGELOG,
        KafkaConfig.STOCK_QUANTITY_CHANGELOG, KafkaConfig.CUSTOMER_MUTABLE_CHANGELOG
    );
    // Topics for immutable stores
    private static final Set<String> storeTopics = ImmutableSet.of(
        KafkaConfig.ITEM_IMMUTABLE_TOPIC, KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC,
        KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC,
        KafkaConfig.STOCK_IMMUTABLE_TOPIC
    );

    // The replication factor that should be used for transaction-related topics
    private static final short TRANSACTION_REPLICATION_FACTOR = 1;
    // The replication factor that should be used for shared stores
    private static final short SHARED_STORE_REPLICATION_FACTOR = 1;

    private AdminClient adminClient;
    private boolean resetImmutableTopics;
    private boolean resetMutableTopics;
    private boolean populate;
    private DatabaseConfig config;

    public Resetter(boolean resetImmutableTopics, boolean resetMutableTopics, boolean populate, DatabaseConfig config) {
        this.resetImmutableTopics = resetImmutableTopics;
        this.resetMutableTopics = resetMutableTopics;
        this.populate = populate;
        this.config = config;

        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

        adminClient = AdminClient.create(adminClientConfig);
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        Arguments arguments = new Arguments(args);
        new Resetter(arguments.getAllFlag(), true, true, arguments.getConfig()).reset();
    }

    public void reset() throws InterruptedException, ExecutionException {
        deleteTopics();
        createTopics();
        if (populate) {
            populateTopics();
        }
    }

    public void deleteTopics() throws InterruptedException, ExecutionException {
        // See which of the topics we care about are already present in Kafka
        Set<String> existingTopics;
        try {
            existingTopics = adminClient.listTopics().names().get(20, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout when trying to connect to Kafka", e);
        }
        Set<String> knownTopics = new HashSet<>();
        if (resetMutableTopics) {
            knownTopics.addAll(mutableTopics);
        }
        if (resetImmutableTopics) {
            knownTopics.addAll(storeTopics);
        }
        existingTopics.retainAll(knownTopics);

        // Then delete all of those topics, to remove all the items and state about them
        DeleteTopicsResult deleteResult = adminClient.deleteTopics(existingTopics);

        // Wait for the deletion to complete, so we don't have problems when creating
        deleteResult.all().get();
    }

    private void createTopics() throws InterruptedException, ExecutionException {
        List<ListenableFuture<Void>> futures = new ArrayList<>(14);

        if (resetImmutableTopics) {
            // Shared stores only have one partition, but should be replicated to allow for broker failures
            futures.add(createTopic(new NewTopic(
                KafkaConfig.ITEM_IMMUTABLE_TOPIC, 1, SHARED_STORE_REPLICATION_FACTOR
            ), adminClient, 0));
            futures.add(createTopic(new NewTopic(
                KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC, 1, SHARED_STORE_REPLICATION_FACTOR
            ), adminClient, 0));
            futures.add(createTopic(new NewTopic(
                KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, 1, SHARED_STORE_REPLICATION_FACTOR
            ), adminClient, 0));
            futures.add(createTopic(new NewTopic(
                KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC, 1, SHARED_STORE_REPLICATION_FACTOR
            ), adminClient, 0));
            futures.add(createTopic(new NewTopic(
                KafkaConfig.STOCK_IMMUTABLE_TOPIC, 1, SHARED_STORE_REPLICATION_FACTOR
            ), adminClient, 0));
        }
        if (resetMutableTopics) {
            // Topics involved with transactions are partitioned based on the warehouse they are associated with
            int transactionRequestPartitions = config.getVerifierInstances() * config.getVerifierThreads() * 8;
            int acceptedTransactionPartitions = config.getWorkerInstances() * config.getWorkerThreads() * 8;

            futures.add(createTopic(new NewTopic(
                KafkaConfig.TRANSACTION_REQUEST_TOPIC, transactionRequestPartitions, TRANSACTION_REPLICATION_FACTOR
            ), adminClient, 0));
            futures.add(createTopic(new NewTopic(
                KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, acceptedTransactionPartitions, TRANSACTION_REPLICATION_FACTOR
            ), adminClient, 0));
            // Modification log probably wants to be partitioned more later
            futures.add(createTopic(new NewTopic(
                KafkaConfig.MODIFICATION_LOG, 1, TRANSACTION_REPLICATION_FACTOR
            ), adminClient, 0));
            // The transaction results can be filtered by the application, but aim to have ~1 partition per application DB connection
            int expectedDbConnections = (config.getWarehouseCount() + 199) / 200; // ceiling division
            futures.add(createTopic(new NewTopic(
                KafkaConfig.TRANSACTION_RESULT_TOPIC, expectedDbConnections, TRANSACTION_REPLICATION_FACTOR
            ), adminClient, 0));

            // Also create worker changelogs, which need to be partitioned the same as the accepted transaction topic
            futures.add(createTopic(new NewTopic(
                KafkaConfig.STOCK_QUANTITY_CHANGELOG, acceptedTransactionPartitions, TRANSACTION_REPLICATION_FACTOR
            ), adminClient, 0));
            futures.add(createTopic(new NewTopic(
                KafkaConfig.NEW_ORDER_CHANGELOG, acceptedTransactionPartitions, TRANSACTION_REPLICATION_FACTOR
            ), adminClient, 0));
            futures.add(createTopic(new NewTopic(
                KafkaConfig.CUSTOMER_MUTABLE_CHANGELOG, acceptedTransactionPartitions, TRANSACTION_REPLICATION_FACTOR
            ), adminClient, 0));
            futures.add(createTopic(new NewTopic(
                KafkaConfig.DISTRICT_NEXT_ORDER_ID_CHANGELOG, acceptedTransactionPartitions, TRANSACTION_REPLICATION_FACTOR
            ), adminClient, 0));
        }

        // Wait for it to take effect
        Futures.allAsList(futures).get();
    }

    // Sometimes the deletion takes a bit of time to take effect properly, so retry with backoff
    private ListenableFuture<Void> createTopic(NewTopic topic, AdminClient adminClient, int attempts) {
        CreateTopicsResult createResult = adminClient.createTopics(List.of(topic));
        ListenableFuture<Void> result = JdkFutureAdapters.listenInPoolThread(createResult.all(), MoreExecutors.directExecutor());
        if (attempts > 5) {
            // No more retries
            return result;
        }
        return Futures.catchingAsync(result, KafkaException.class, e -> {
            Thread.sleep(200 * (attempts + 1));
            return createTopic(topic, adminClient, attempts + 1);
        }, MoreExecutors.directExecutor());
    }

    private void populateTopics() {
        try (PopulateStores populateStores = new PopulateStores(config, resetImmutableTopics, resetMutableTopics)
        ) {
            populateStores.populate();
        }
    }

    @Override
    public void close() {
        adminClient.close();
    }
}
