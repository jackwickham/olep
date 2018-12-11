package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.message.transaction_result.TransactionResult;
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

import java.io.Closeable;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static net.jackw.olep.common.KafkaConfig.TRANSACTION_REQUEST_TOPIC;
import static net.jackw.olep.common.KafkaConfig.TRANSACTION_RESULT_TOPIC;

/**
 * A connection to the OLEP database
 */
public class DatabaseConnection implements Closeable {
    private final Producer<Long, TransactionRequestMessage> transactionRequestProducer;
    private final Consumer<Long, byte[]> transactionResultConsumer;

    private final Map<Long, PendingTransaction<?, ?>> pendingTransactions;

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

        // Set up the producer, which is used to send requests from the application to the DB
        Serializer<TransactionRequestMessage> transactionRequestSerializer = new JsonSerializer<>(TransactionRequestMessage.class);
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
            transactionResultConsumerProps, Serdes.Long().deserializer(), Serdes.ByteArray().deserializer()
        );
        // Assign to the correct partition
        transactionResultConsumer.assign(List.of(new TopicPartition(
            TRANSACTION_RESULT_TOPIC,
            getPartition(transactionResultConsumer.partitionsFor(TRANSACTION_RESULT_TOPIC).size())
        )));

        // Create the transaction result processor too
        transactionResultProcessor = new TransactionResultProcessor(pendingTransactions);

        // Start a thread that listens to the result log and processes the results
        resultThread = new Thread(this::processRecords);
        resultThread.start();
    }

    @Override
    public void close() {
        System.out.println("Closing");
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
    private <T extends TransactionResult, B extends TransactionResultBuilder<T>> PendingTransaction<T, B> send(
        TransactionRequestMessage msg, B resultBuilder
    ) {
        long transactionId = nextTransactionId();
        System.out.printf("Sending transaction %d\n", transactionId);

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
                ConsumerRecords<Long, byte[]> records = transactionResultConsumer.poll(Duration.ofHours(12));
                for (ConsumerRecord<Long, byte[]> record : records) {
                    if (connectionId == (int)(long)record.key()) {
                        transactionResultProcessor.process(record.value());
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

    /**
     * Get the partition that transaction results will be written to
     *
     * @param numPartitions The total number of transaction result partitions
     * @return The correct partition
     */
    private int getPartition(int numPartitions) {
        return (int)(((long)connectionId & 0xFFFFFFFFL) % numPartitions);
    }
}
