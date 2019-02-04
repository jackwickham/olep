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
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SharedStoreConsumerTest {
    private static final String TOPIC = "test-mock-topic";

    private MockConsumer<String, String> consumer;
    private ConcreteStoreConsumer storeConsumer;

    @Mock
    private WritableKeyValueStore<String, String> store;

    @Before
    public void setup() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updateBeginningOffsets(Map.of(new TopicPartition(TOPIC, 0), 0L));

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
        Thread.sleep(30);

        verifyZeroInteractions(store);
    }

    @Test
    public void testInsertsRecordsIntoStore() throws InterruptedException {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "key0", "val0"));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "key1", "val1"));

        // Allow the consumer thread time to process it
        Thread.sleep(30);

        verify(store).put("key0", "val0");
        verify(store).put("key1", "val1");
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testRemoveRecordsFromStore() throws InterruptedException {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "key0", null));

        // Allow the consumer thread time to process it
        Thread.sleep(30);

        verify(store).remove("key0");
        verifyNoMoreInteractions(store);
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
