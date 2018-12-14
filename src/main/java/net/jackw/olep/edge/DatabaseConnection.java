package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.LRUSet;
import net.jackw.olep.common.TransactionResultPartitioner;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import net.jackw.olep.message.transaction_result.TransactionResultBuilder;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static net.jackw.olep.common.KafkaConfig.TRANSACTION_REQUEST_TOPIC;
import static net.jackw.olep.common.KafkaConfig.TRANSACTION_RESULT_TOPIC;

/**
 * A connection to the OLEP database
 */
public class DatabaseConnection implements Closeable {
    private final Producer<Long, TransactionRequestMessage> transactionRequestProducer;
    private final Consumer<TransactionResultKey, byte[]> transactionResultConsumer;

    private final Map<Long, PendingTransaction<?, ?>> pendingTransactions;
    // Store recently completed transactions so duplicate messages about them can be discarded without reporting errors
    private final Set<Long> recentlyCompletedTransactions;

    private final Thread resultThread;

    // Takes a byte[] and updates the corresponding PendingTransaction
    private final TransactionResultProcessor transactionResultProcessor;

    // Use a CSPRNG to generate connection IDs to avoid collisions
    private static final Random rand = new SecureRandom();
    private final int connectionId;


    @MustBeClosed
    public DatabaseConnection(String bootstrapServers) {
        connectionId = rand.nextInt();

        // Initialise regular fields
        pendingTransactions = new ConcurrentHashMap<>();
        recentlyCompletedTransactions = new LRUSet<>(100);

        // Set up the producer, which is used to send requests from the application to the DB
        Serializer<TransactionRequestMessage> transactionRequestSerializer = new JsonSerializer<>();
        Properties transactionRequestProducerProps = new Properties();
        transactionRequestProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionRequestProducerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        transactionRequestProducer = new KafkaProducer<>(
            transactionRequestProducerProps, Serdes.Long().serializer(), transactionRequestSerializer
        );

        // Set up the consumer, which is used to receive transaction result messages from the DB
        // The body is initially only decoded as a byte[], because we can only decode it if we sent the corresponding
        // transaction, and we only know that once the key has been decoded
        Properties transactionResultConsumerProps = new Properties();
        transactionResultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionResultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "result-consumer" + connectionId);
        transactionResultConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        transactionResultConsumer = new KafkaConsumer<>(
            transactionResultConsumerProps,
            new TransactionResultKey.ResultKeyDeserializer(),
            Serdes.ByteArray().deserializer()
        );
        // Assign to the correct partition
        TopicPartition partition = new TopicPartition(
            TRANSACTION_RESULT_TOPIC,
            TransactionResultPartitioner.partition(
                connectionId, transactionResultConsumer.partitionsFor(TRANSACTION_RESULT_TOPIC).size()
            )
        );
        transactionResultConsumer.assign(List.of(partition));
        // And seek to the end, since we've not sent any transactions yet
        transactionResultConsumer.seekToEnd(List.of(partition));

        // Create the transaction result processor, so we can decode the results
        transactionResultProcessor = new TransactionResultProcessor();

        // Start a thread that listens to the result log and processes the results
        resultThread = new Thread(this::processRecords);
        resultThread.start();
    }

    @Override
    public void close() {
        log.info("Closing");
        try {
            // Stop the listener if possible
            if (resultThread.isAlive()) {
                transactionResultConsumer.wakeup();
                resultThread.join(10000);
            }
        } catch (InterruptedException e) {
            // Convert to an unchecked exception
            throw new InterruptException(e);
        } finally {
            // Then close the producer and consumer
            transactionRequestProducer.close();
            transactionResultConsumer.close();
        }
    }

    /**
     * Send a New-Order transaction
     */
    public TransactionStatus<NewOrderResult> newOrder(
        int customerId, int warehouseId, int districtId, List<NewOrderRequest.OrderLine> lines
    ) {
        long orderDate = new Date().getTime();
        NewOrderRequest msgBody = new NewOrderRequest(
            customerId, warehouseId, districtId, ImmutableList.copyOf(lines), orderDate
        );
        return send(msgBody, new NewOrderResult.Builder(warehouseId, districtId, customerId, orderDate, lines))
            .getTransactionStatus();
    }

    /**
     * Send a Payment transaction by customer ID
     */
    public TransactionStatus<PaymentResult> payment(
        int warehouseId, int districtId, int customerWarehouseId, int customerDistrictId, int customerId,
        BigDecimal amount
    ) {
        PaymentRequest msgBody = new PaymentRequest(
            warehouseId, districtId, customerId, customerWarehouseId, customerDistrictId, amount
        );
        return send(msgBody, new PaymentResult.Builder(
            warehouseId, districtId, customerWarehouseId, customerDistrictId, customerId
        )).getTransactionStatus();
    }

    /**
     * Send a Payment transaction by customer last name
     */
    public TransactionStatus<PaymentResult> payment(
        int warehouseId, int districtId, int customerWarehouseId, int customerDistrictId, String customerSurname,
        BigDecimal amount
    ) {
        PaymentRequest msgBody = new PaymentRequest(
            warehouseId, districtId, customerSurname, customerWarehouseId, customerDistrictId, amount
        );
        return send(msgBody, new PaymentResult.Builder(
            warehouseId, districtId, customerWarehouseId, customerDistrictId
        )).getTransactionStatus();
    }

    /**
     * Send a Delivery transaction
     */
    public TransactionStatus<DeliveryResult> delivery(int warehouseId, int carrierId) {
        DeliveryRequest msgBody = new DeliveryRequest(warehouseId, carrierId, new Date().getTime());
        return send(msgBody, new DeliveryResult.Builder(warehouseId, carrierId)).getTransactionStatus();
    }

    /**
     * Deliver a message to Kafka
     *
     * @param msg The message to send
     */
    @SuppressWarnings("FutureReturnValueIgnored")
    private <T extends TransactionResultMessage, B extends TransactionResultBuilder<T>> PendingTransaction<T, B> send(
        TransactionRequestMessage msg, B resultBuilder
    ) {
        long transactionId = nextTransactionId();
        log.debug("Sending {} transaction {}", msg.getClass().getSimpleName(), transactionId);

        PendingTransaction<T, B> pendingTransaction = new PendingTransaction<>(transactionId, resultBuilder);
        pendingTransactions.put(transactionId, pendingTransaction);

        // Publish to Kafka, and provide the writtenToLogCallback to Kafka to call once that's done
        transactionRequestProducer.send(
            new ProducerRecord<>(TRANSACTION_REQUEST_TOPIC, transactionId, msg),
            pendingTransaction.getWrittenToLogCallback()
        );

        return pendingTransaction;
    }

    /**
     * Run until the thread is interrupted, processing records from the result log
     */
    private void processRecords() {
        while (true) {
            try {
                ConsumerRecords<TransactionResultKey, byte[]> records = transactionResultConsumer.poll(Duration.ofHours(12));
                for (ConsumerRecord<TransactionResultKey, byte[]> record : records) {
                    long transactionId = record.key().transactionId;
                    if (connectionId == record.key().getConnectionId()) {
                        // It's for us!
                        PendingTransaction<?, ?> pendingTransaction = pendingTransactions.get(transactionId);
                        if (pendingTransaction == null) {
                            if (!recentlyCompletedTransactions.contains(transactionId)) {
                                log.warn("Received a transaction result for a transaction that isn't pending or recently completed");
                            } else {
                                // Otherwise it's a duplicate, so we can just ignore it
                                log.debug("Received duplicate result for completed transaction {}", transactionId);
                            }
                        } else {
                            log.debug("Received message");
                            boolean complete = transactionResultProcessor.process(
                                record.key().approvalMessage, record.value(), pendingTransaction
                            );

                            if (complete) {
                                pendingTransactions.remove(transactionId);
                                recentlyCompletedTransactions.add(transactionId);
                            }
                        }
                    }
                }
            } catch (InterruptException | WakeupException e) {
                break;
            }
        }
    }

    private int lastTransactionId = 0;

    /**
     * Generate a new transaction ID
     *
     * @return A globally unique transaction ID
     */
    private long nextTransactionId() {
        return (Integer.toUnsignedLong(++lastTransactionId) << 32) | Integer.toUnsignedLong(connectionId);
    }

    private static Logger log = LogManager.getLogger();
}
