package net.jackw.olep.common.store;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SharedStoreConsumerTest {
    private static final String TOPIC = "test-mock-topic";
    private static final int TIMEOUT = 30;

    private MockConsumer<String, String> consumer;
    private ConcreteStoreConsumer storeConsumer;

    @Mock
    private WritableKeyValueStore<String, String> store;

    @Before
    public void setup() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updateBeginningOffsets(Map.of(new TopicPartition(TOPIC, 0), 0L));
        // Initially, expect there to be one message in the topic
        consumer.updateEndOffsets(Map.of(new TopicPartition(TOPIC, 0), 1L));

        storeConsumer = new ConcreteStoreConsumer(consumer);
        storeConsumer.start();
    }

    @After
    public void teardown() throws InterruptedException {
        // Stop the consumer thread
        storeConsumer.close();
    }

    @Test
    public void testDoesntInsertAnythingIntoStoreWhenNothingInTopic() throws InterruptedException {
        // All we need to do is wait a bit and make sure there have been no interactions with the store
        Thread.sleep(TIMEOUT);

        verifyZeroInteractions(store);
    }

    @Test
    public void testInsertsRecordsIntoStore() throws InterruptedException, TimeoutException, ExecutionException {
        consumer.updateEndOffsets(Map.of(new TopicPartition(TOPIC, 0), 2L));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "key0", "val0"));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "key1", "val1"));

        // Allow the consumer thread time to process it
        storeConsumer.getReadyFuture().get(TIMEOUT, TimeUnit.MILLISECONDS);

        verify(store).put("key0", "val0");
        verify(store).put("key1", "val1");
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testRemoveRecordsFromStore() throws InterruptedException, TimeoutException, ExecutionException {
        consumer.updateEndOffsets(Map.of(new TopicPartition(TOPIC, 0), 1L));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "key0", null));

        // Allow the consumer thread time to process it
        storeConsumer.getReadyFuture().get(TIMEOUT, TimeUnit.MILLISECONDS);

        verify(store).remove("key0");
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testReadyFutureDoesntCompleteWhenMessagesPending() throws InterruptedException, TimeoutException, ExecutionException {
        consumer.updateEndOffsets(Map.of(new TopicPartition(TOPIC, 0), 2L));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "key0", "val0"));

        // Allow the consumer thread time to process it
        Thread.sleep(TIMEOUT);

        assertFalse(storeConsumer.getReadyFuture().isDone());
    }

    @Test
    public void testReadyFutureCompletesWhenAllMessagesArrive() throws InterruptedException, TimeoutException, ExecutionException {
        consumer.updateEndOffsets(Map.of(new TopicPartition(TOPIC, 0), 2L));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "key0", "val0"));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "key1", "val1"));

        // Assert that the future completes
        storeConsumer.getReadyFuture().get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private class ConcreteStoreConsumer extends SharedStoreConsumer<String, String> {
        public ConcreteStoreConsumer(Consumer<String, String> consumer) {
            super(consumer, "node", TOPIC);
        }

        @Override
        protected WritableKeyValueStore<String, String> getWriteableStore() {
            // From outer class
            return store;
        }
    }
}
