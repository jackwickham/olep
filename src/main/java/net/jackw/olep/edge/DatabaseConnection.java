package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.edge.transaction_result.DeliveryResult;
import net.jackw.olep.edge.transaction_result.NewOrderResult;
import net.jackw.olep.edge.transaction_result.PaymentResult;
import net.jackw.olep.edge.transaction_result.TestResult;
import net.jackw.olep.edge.transaction_result.TransactionResult;
import net.jackw.olep.edge.transaction_result.TransactionResultBuilder;
import net.jackw.olep.edge.transaction_result.TransactionResultDeserializer;
import net.jackw.olep.message.DeliveryMessage;
import net.jackw.olep.message.NewOrderMessage;
import net.jackw.olep.message.PaymentMessage;
import net.jackw.olep.message.TestMessage;
import net.jackw.olep.message.TransactionRequestBody;
import net.jackw.olep.message.TransactionRequestMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.math.BigDecimal;
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
    private final Consumer<Long, PendingTransaction> transactionResultConsumer;

    private final Serializer<TransactionRequestMessage> transactionRequestSerializer;
    private final Deserializer<PendingTransaction> transactionResultDeserializer;

    private final Map<Long, PendingTransaction<?, ?>> pendingTransactions;

    private final Thread resultThread;


    @MustBeClosed
    public DatabaseConnection(String bootstrapServers) {
        // Initialise regular fields
        pendingTransactions = new ConcurrentHashMap<>();

        // Set up the producer, which is used to send requests from the application to the DB
        transactionRequestSerializer = new JsonSerializer<>(TransactionRequestMessage.class);
        Properties transactionRequestProducerProps = new Properties();
        transactionRequestProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionRequestProducerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        transactionRequestProducer = new KafkaProducer<>(transactionRequestProducerProps, Serdes.Long().serializer(), transactionRequestSerializer);

        // Set up the consumer, which is used to receive transaction result messages from the DB
        transactionResultDeserializer = new TransactionResultDeserializer(pendingTransactions);
        Properties transactionResultConsumerProps = new Properties();
        transactionResultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        transactionResultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "result-consumer" + rand.nextInt()); // TODO: this probably needs to be unique per app instance
        transactionResultConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        transactionResultConsumer = new KafkaConsumer<>(transactionResultConsumerProps, Serdes.Long().deserializer(), transactionResultDeserializer);
        transactionResultConsumer.subscribe(List.of(TRANSACTION_RESULT_TOPIC));

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
     * Send a test transaction
     *
     * @param body The message to send in the transaction
     */
    public TransactionStatus<TestResult> test(String body, int item) {
        TestMessage msgBody = new TestMessage(body, item);
        return send(msgBody, new TestResult.Builder()).getTransactionStatus();
    }

    /**
     * Send a New-Order transaction
     */
    public TransactionStatus<NewOrderResult> newOrder(
        int customerId, int warehouseId, int districtId, List<NewOrderMessage.OrderLine> lines
    ) {
        NewOrderMessage msgBody = new NewOrderMessage(
            customerId, warehouseId, districtId, ImmutableList.copyOf(lines), new Date().getTime()
        );
        return send(msgBody, new NewOrderResult.Builder()).getTransactionStatus();
    }

    /**
     * Send a Payment transaction by customer ID
     */
    public TransactionStatus<PaymentResult> payment(
        int warehouseId, int districtId, int customerWarehouseId, int customerDistrictId, int customerId,
        BigDecimal amount
    ) {
        PaymentMessage msgBody = new PaymentMessage(
            warehouseId, districtId, customerId, customerWarehouseId, customerDistrictId, amount
        );
        return send(msgBody, new PaymentResult.Builder()).getTransactionStatus();
    }

    /**
     * Send a Payment transaction by customer last name
     */
    public TransactionStatus<PaymentResult> payment(
        int warehouseId, int districtId, int customerWarehouseId, int customerDistrictId, String customerSurname,
        BigDecimal amount
    ) {
        PaymentMessage msgBody = new PaymentMessage(
            warehouseId, districtId, customerSurname, customerWarehouseId, customerDistrictId, amount
        );
        return send(msgBody, new PaymentResult.Builder()).getTransactionStatus();
    }

    public TransactionStatus<DeliveryResult> delivery(int warehouseId, int carrierId) {
        DeliveryMessage msgBody = new DeliveryMessage(warehouseId, carrierId, new Date().getTime());
        return send(msgBody, new DeliveryResult.Builder()).getTransactionStatus();
    }

    /**
     * Deliver a message to Kafka
     *
     * @param msgBody The message to send
     */
    @SuppressWarnings("FutureReturnValueIgnored")
    private <T extends TransactionResult, B extends TransactionResultBuilder<T>> PendingTransaction<T, B> send(TransactionRequestBody msgBody, B resultBuilder) {
        long transactionId = nextTransactionId();

        PendingTransaction<T, B> pendingTransaction = new PendingTransaction<>(transactionId, resultBuilder);
        pendingTransactions.put(transactionId, pendingTransaction);

        TransactionRequestMessage msg = new TransactionRequestMessage(transactionId, msgBody);
        transactionRequestProducer.send(new ProducerRecord<>(TRANSACTION_REQUEST_TOPIC, transactionId, msg), pendingTransaction.getWrittenToLogCallback());

        return pendingTransaction;
    }

    /**
     * Run until the thread is interrupted, processing records from the result log
     */
    private void processRecords() {
        while (true) {
            try {
                transactionResultConsumer.poll(Duration.ofHours(12));
            } catch (InterruptException | WakeupException e) {
                break;
            }
        }
    }

    private static Random rand = new Random();

    /**
     * Generate a new transaction ID
     *
     * This has been abstracted into a method so different methods, such as snowflakes, can be used in future.
     *
     * @return A globally unique transaction ID
     */
    private long nextTransactionId() {
        return rand.nextLong();
    }
}
