package net.jackw.olep.utils;

import net.jackw.olep.common.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ClusterCreator {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int numVerifiers = 2;
        int numWorkers = 2;
        int numApplications = 2;

        // The replication factor that should be used for transaction-related topics
        short transactionReplicationFactor = 1;
        // The replication factor that should be used for shared stores
        short sharedStoreReplicationFactor = 1;

        Set<String> dbTopics = Set.of(
            // Transaction topics
            KafkaConfig.TRANSACTION_REQUEST_TOPIC, KafkaConfig.TRANSACTION_RESULT_TOPIC,
            KafkaConfig.ACCEPTED_TRANSACTION_TOPIC,
            // Shared store topics
            KafkaConfig.ITEM_IMMUTABLE_TOPIC, KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC,
            KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC,
            KafkaConfig.STOCK_IMMUTABLE_TOPIC
        );

        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(adminClientConfig)) {
            // See which of the topics we care about are already present in Kafka
            Set<String> existingTopics = adminClient.listTopics().names().get();
            existingTopics.retainAll(dbTopics);
            // Then delete all of those topics, to remove all the items and state about them
            DeleteTopicsResult deleteResult = adminClient.deleteTopics(existingTopics);

            // Wait for the deletion to complete, so we don't have problems when creating
            deleteResult.all().get();
            // Sometimes they aren't properly deleted yet, so wait until they are
            Thread.sleep(200);
            // However, the listTopics endpoint doesn't include the topics that were only partly deleted, so we can't
            // actually check whether it is ready

            // Now define the topics to be created
            List<NewTopic> topicsToCreate = new ArrayList<>(4);

            // Shared stores only have one partition, but should be replicated to allow for broker failures
            topicsToCreate.add(new NewTopic(KafkaConfig.ITEM_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor));
            topicsToCreate.add(new NewTopic(KafkaConfig.WAREHOUSE_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor));
            topicsToCreate.add(new NewTopic(KafkaConfig.DISTRICT_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor));
            topicsToCreate.add(new NewTopic(KafkaConfig.CUSTOMER_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor));
            topicsToCreate.add(new NewTopic(KafkaConfig.STOCK_IMMUTABLE_TOPIC, 1, sharedStoreReplicationFactor));
            // Topics involved with transactions are partitioned based on the warehouse they are associated with
            // To allow for scaling if needed, have twice as many partitions as verifiers/workers
            topicsToCreate.add(new NewTopic(KafkaConfig.TRANSACTION_REQUEST_TOPIC, numVerifiers * 2, transactionReplicationFactor));
            topicsToCreate.add(new NewTopic(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, numWorkers * 2, transactionReplicationFactor));
            // Modification log probably wants to be partitioned more later
            topicsToCreate.add(new NewTopic(KafkaConfig.MODIFICATION_LOG, 1, transactionReplicationFactor));
            // The transaction results can be filtered by the application, but aim to have ~1 partition per application
            topicsToCreate.add(new NewTopic(KafkaConfig.TRANSACTION_RESULT_TOPIC, numApplications, transactionReplicationFactor));

            // Make it happen
            CreateTopicsResult createResult = adminClient.createTopics(topicsToCreate);
            // Wait for the changes to propagate to all nodes
            createResult.all().get();
        }
    }
}
