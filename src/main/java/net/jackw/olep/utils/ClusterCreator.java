package net.jackw.olep.utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import net.jackw.olep.common.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClusterCreator {
    private static Set<String> transactionTopics = ImmutableSet.of(
        KafkaConfig.TRANSACTION_REQUEST_TOPIC, KafkaConfig.TRANSACTION_RESULT_TOPIC,
        KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, KafkaConfig.MODIFICATION_LOG
    );
    private static Set<String> storeTopics = ImmutableSet.of(
        KafkaConfig.ITEM_IMMUTABLE_TOPIC, KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC,
        KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC,
        KafkaConfig.STOCK_IMMUTABLE_TOPIC
    );

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        resetAll();
    }

    public static void resetAll() throws InterruptedException, ExecutionException {
        new ClusterCreator().reset(true, true, true);
    }

    public static void resetKnownTopics() throws InterruptedException, ExecutionException {
        new ClusterCreator().reset(true, true, false);
    }

    public static void resetTransactionTopics() throws InterruptedException, ExecutionException {
        new ClusterCreator().reset(true, false, false);
    }

    private void reset(boolean resetTransactionTopics, boolean resetStoreTopics, boolean resetOtherTopics) throws InterruptedException, ExecutionException {
        int numVerifiers = 2;
        int numWorkers = 2;
        int numApplications = 2;

        // The replication factor that should be used for transaction-related topics
        short transactionReplicationFactor = 1;
        // The replication factor that should be used for shared stores
        short sharedStoreReplicationFactor = 1;

        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(adminClientConfig)) {
            // See which of the topics we care about are already present in Kafka
            Set<String> existingTopics;
            try {
                 existingTopics = adminClient.listTopics().names().get(20, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new RuntimeException("Timeout when trying to connect to Kafka", e);
            }
            if (!resetOtherTopics) {
                Set<String> dbTopics = new HashSet<>();
                if (resetTransactionTopics) {
                    dbTopics.addAll(transactionTopics);
                }
                if (resetStoreTopics) {
                    dbTopics.addAll(storeTopics);
                }
                existingTopics.retainAll(dbTopics);
            } else if (!resetTransactionTopics || !resetStoreTopics) {
                throw new IllegalArgumentException();
            }

            // Then delete all of those topics, to remove all the items and state about them
            DeleteTopicsResult deleteResult = adminClient.deleteTopics(existingTopics);

            // Wait for the deletion to complete, so we don't have problems when creating
            deleteResult.all().get();

            List<ListenableFuture<Void>> futures = new ArrayList<>(9);

            if (resetStoreTopics) {
                // Shared stores only have one partition, but should be replicated to allow for broker failures
                futures.add(create(new NewTopic(KafkaConfig.ITEM_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor), adminClient, 0));
                futures.add(create(new NewTopic(KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor), adminClient, 0));
                futures.add(create(new NewTopic(KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor), adminClient, 0));
                futures.add(create(new NewTopic(KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor), adminClient, 0));
                futures.add(create(new NewTopic(KafkaConfig.STOCK_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor), adminClient, 0));
            }
            if (resetTransactionTopics) {
                // Topics involved with transactions are partitioned based on the warehouse they are associated with
                // To allow for scaling if needed, have twice as many partitions as verifiers/workers
                futures.add(create(new NewTopic(KafkaConfig.TRANSACTION_REQUEST_TOPIC, numVerifiers * 2, transactionReplicationFactor), adminClient, 0));
                futures.add(create(new NewTopic(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, numWorkers * 2, transactionReplicationFactor), adminClient, 0));
                // Modification log probably wants to be partitioned more later
                futures.add(create(new NewTopic(KafkaConfig.MODIFICATION_LOG, 1, transactionReplicationFactor), adminClient, 0));
                // The transaction results can be filtered by the application, but aim to have ~1 partition per application
                futures.add(create(new NewTopic(KafkaConfig.TRANSACTION_RESULT_TOPIC, numApplications, transactionReplicationFactor), adminClient, 0));
            }

            // Wait for it to take effect
            Futures.allAsList(futures).get();
        }
    }

    // Sometimes the deletion takes a bit of time to take effect properly, so retry with backoff
    private ListenableFuture<Void> create(NewTopic topic, AdminClient adminClient, int attempts) {
        CreateTopicsResult createResult = adminClient.createTopics(List.of(topic));
        ListenableFuture<Void> result = JdkFutureAdapters.listenInPoolThread(createResult.all(), MoreExecutors.directExecutor());
        if (attempts > 5) {
            // No more retries
            return result;
        }
        return Futures.catchingAsync(result, KafkaException.class, e -> {
            Thread.sleep(200 * (attempts + 1));
            return create(topic, adminClient, attempts + 1);
        }, MoreExecutors.directExecutor());
    }
}
