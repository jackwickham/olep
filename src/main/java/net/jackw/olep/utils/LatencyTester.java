package net.jackw.olep.utils;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.TransactionRequestMessage;
import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import net.jackw.olep.message.transaction_result.TransactionResultKey;
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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Measure the latency of a transaction at each stage in the transaction pipeline
 */
public class LatencyTester implements AutoCloseable {
    private Consumer<TransactionResultKey, byte[]> resultConsumer;
    private Consumer<TransactionWarehouseKey, byte[]> acceptedTransactionConsumer;
    private Producer<Long, TransactionRequestMessage> transactionProducer;
    private Map<Long, TransactionState> transactions;
    private Thread mainThread;
    private int prevTransactionIndex = 0;

    public LatencyTester(DatabaseConfig config) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "latency-tester");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Result log consumer
        resultConsumer = new KafkaConsumer<>(
            consumerProps, new TransactionResultKey.ResultKeyDeserializer(), Serdes.ByteArray().deserializer()
        );
        int numResultPartitions = (config.getWarehouseCount() + 199) / 200;
        List<TopicPartition> resultPartitions = new ArrayList<>(numResultPartitions);
        for (int i = 0; i < numResultPartitions; i++) {
            resultPartitions.add(new TopicPartition(KafkaConfig.TRANSACTION_RESULT_TOPIC, i));
        }
        resultConsumer.assign(resultPartitions);
        resultConsumer.seekToEnd(resultPartitions);

        // Accepted transaction log consumer
        acceptedTransactionConsumer = new KafkaConsumer<>(
            consumerProps, new TransactionWarehouseKey.KeyDeserializer(), Serdes.ByteArray().deserializer()
        );
        int numModificationPartitions = config.getWorkerInstances() * config.getWorkerThreads() * 8;
        List<TopicPartition> acceptedTransactionPartitions = new ArrayList<>(numModificationPartitions);
        for (int i = 0; i < numModificationPartitions; i++) {
            acceptedTransactionPartitions.add(new TopicPartition(KafkaConfig.ACCEPTED_TRANSACTION_TOPIC, i));
        }
        acceptedTransactionConsumer.assign(acceptedTransactionPartitions);
        acceptedTransactionConsumer.seekToEnd(acceptedTransactionPartitions);

        // Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        transactionProducer = new KafkaProducer<>(producerProps, Serdes.Long().serializer(), new JsonSerializer<>());

        transactions = new ConcurrentHashMap<>();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            resultConsumer.wakeup();
            acceptedTransactionConsumer.wakeup();
            mainThread.interrupt();
            try {
                mainThread.join();
            } catch (InterruptedException e) { }
        }));
    }

    public void start() {
        this.mainThread = Thread.currentThread();

        new Thread(this::consumeResultLog, "Result log consumer").start();
        new Thread(this::consumeModificationLog, "Modification log consumer").start();

        try {
            while (true) {
                long transactionId = (1L << 32) | ++prevTransactionIndex;
                DeliveryRequest transactionBody = new DeliveryRequest(1, 1, 1L);
                TransactionState state = new TransactionState(transactionId, prevTransactionIndex);
                transactions.put(transactionId, state);
                transactionProducer.send(new ProducerRecord<>(KafkaConfig.TRANSACTION_REQUEST_TOPIC, 0, transactionId, transactionBody), (_a, _b) -> {
                    state.onWrittenToRequestLog(System.nanoTime());
                });

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            transactionProducer.close();
        }
    }

    private void consumeResultLog() {
        try {
            while (true) {
                ConsumerRecords<TransactionResultKey, byte[]> msgs = resultConsumer.poll(Duration.ofHours(12));

                long time = System.nanoTime();
                for (ConsumerRecord<TransactionResultKey, byte[]> msg : msgs) {
                    TransactionState state = transactions.get(msg.key().transactionId);
                    if (state != null) {
                        if (msg.key().approvalMessage) {
                            state.onAccepted(time);
                        } else {
                            state.onReceivedResult(time);
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            resultConsumer.close();
        }
    }

    private void consumeModificationLog() {
        try {
            while (true) {
                ConsumerRecords<TransactionWarehouseKey, byte[]> msgs = acceptedTransactionConsumer.poll(Duration.ofHours(12));

                long time = System.nanoTime();
                for (ConsumerRecord<TransactionWarehouseKey, byte[]> msg : msgs) {
                    TransactionState state = transactions.get(msg.key().transactionId);
                    if (state != null) {
                        state.onReceivedModification(time);
                    }
                }
            }
        } catch (WakeupException e) {
            acceptedTransactionConsumer.close();
        }
    }

    @Override
    public void close() {
        resultConsumer.wakeup();
        acceptedTransactionConsumer.wakeup();

    }

    public static void main(String[] args) throws IOException {
        DatabaseConfig config = DatabaseConfig.create(args);
        new LatencyTester(config).start();
    }

    private class TransactionState {
        private int index;
        private long id;

        private long zero;

        private double writtenToRequestLog = Double.NaN;
        private double receivedAcceptance = Double.NaN;
        private double receivedModification = Double.NaN;
        private double receivedResult = Double.NaN;

        public TransactionState(long id, int index) {
            this.id = id;
            this.index = index;
            this.zero = System.nanoTime();
        }

        public synchronized void onWrittenToRequestLog(long receivedTime) {
            writtenToRequestLog = (receivedTime - zero) / 1000000.0;
            onUpdate();
        }

        public synchronized void onAccepted(long receivedTime) {
            receivedAcceptance = (receivedTime - zero) / 1000000.0;
            onUpdate();
        }

        public synchronized void onReceivedModification(long receivedTime) {
            receivedModification = (receivedTime - zero) / 1000000.0;
            onUpdate();
        }

        public synchronized void onReceivedResult(long receivedTime) {
            receivedResult = (receivedTime - zero) / 1000000.0;
            onUpdate();
        }

        private void onUpdate() {
            if (
                !Double.isNaN(writtenToRequestLog) && !Double.isNaN(receivedAcceptance) &&
                !Double.isNaN(receivedModification) && !Double.isNaN(receivedResult)
            ) {
                // Done
                transactions.remove(id);
                System.out.printf(
                    "%4d: %8.3fms %8.3fms %8.3fms %8.3fms\n",
                    index, writtenToRequestLog, receivedAcceptance, receivedModification, receivedResult
                );
            }
        }
    }
}
