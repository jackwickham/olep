package net.jackw.olep.view;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.store.SharedCustomerStoreConsumer;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.ModificationKey;
import net.jackw.olep.message.modification.ModificationMessage;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import net.jackw.olep.metrics.InMemoryMetrics;
import net.jackw.olep.metrics.Metrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class LogViewAdapterTest {
    private MockConsumer<ModificationKey, ModificationMessage> logConsumer;
    private Metrics metrics = new InMemoryMetrics();
    private SettableFuture<Void> customerStoreConsumerReadyFuture = SettableFuture.create();
    private TopicPartition topicPartition = new TopicPartition(KafkaConfig.MODIFICATION_LOG, 0);
    private TopicPartition secondaryTopicPartition = new TopicPartition(KafkaConfig.MODIFICATION_LOG, 1);

    @Mock
    private ViewWriteAdapter viewAdapter;

    @Mock
    private SharedCustomerStoreConsumer customerStoreConsumer;

    private LogViewAdapter lva;

    @Before
    public void setup() {
        logConsumer = spy(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        logConsumer.updatePartitions(KafkaConfig.MODIFICATION_LOG, List.of(new PartitionInfo(
            KafkaConfig.MODIFICATION_LOG, 0, null, null, null
        ), new PartitionInfo(
            KafkaConfig.MODIFICATION_LOG, 1, null, null, null
        )));
        logConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L, secondaryTopicPartition, 0L));
        logConsumer.updateEndOffsets(Map.of(topicPartition, 3L, secondaryTopicPartition, 3L));

        when(customerStoreConsumer.getReadyFuture()).thenReturn(customerStoreConsumerReadyFuture);

        lva = new LogViewAdapter(logConsumer, viewAdapter, customerStoreConsumer, metrics);

        // Need to call rebalance so that the topic actually gets assigned to lva
        logConsumer.rebalance(List.of(topicPartition, secondaryTopicPartition));
    }

    @After
    public void teardown() {
        logConsumer.wakeup();
    }

    @Test
    public void testDoesntPollUntilCustomerStoreIsReady() throws InterruptedException {
        lva.start();

        Thread.sleep(20);

        verify(logConsumer, never()).poll(any());

        customerStoreConsumerReadyFuture.set(null);

        Thread.sleep(20);

        verify(logConsumer, atLeastOnce()).poll(any());
    }

    @Test(timeout = 500)
    public void testThreadAbortsIfFutureRejected() throws InterruptedException {
        customerStoreConsumerReadyFuture.setException(new Exception());
        lva.start();
        lva.join();
    }

    @Test
    public void testNewOrderModificationSentToViewAdapter() throws InterruptedException {
        NewOrderModification modification = new NewOrderModification(
            1, 2, 3, ImmutableList.of(), 100L, 10
        );
        logConsumer.addRecord(new ConsumerRecord<>(
            KafkaConfig.MODIFICATION_LOG, 0, 1, new ModificationKey(1L, (short) 1), modification
        ));

        customerStoreConsumerReadyFuture.set(null);
        lva.start();

        Thread.sleep(30);

        verify(viewAdapter).newOrder(same(modification));
    }

    @Test
    public void testRemoteStockModificationSentToViewAdapter() throws InterruptedException {
        RemoteStockModification modification = new RemoteStockModification(1, 2, 3);
        logConsumer.addRecord(new ConsumerRecord<>(
            KafkaConfig.MODIFICATION_LOG, 0, 1, new ModificationKey(1L, (short) 1), modification
        ));

        customerStoreConsumerReadyFuture.set(null);
        lva.start();

        Thread.sleep(30);

        verify(viewAdapter).remoteStock(same(modification));
    }

    @Test
    public void testPaymentModificationSentToViewAdapter() throws InterruptedException {
        PaymentModification modification = new PaymentModification(
            1, 2, 3, 4, 5, new BigDecimal("12.34"),
            new BigDecimal("56.78"), "data"
        );
        logConsumer.addRecord(new ConsumerRecord<>(
            KafkaConfig.MODIFICATION_LOG, 0, 1, new ModificationKey(1L, (short) 1), modification
        ));

        customerStoreConsumerReadyFuture.set(null);
        lva.start();

        Thread.sleep(30);

        verify(viewAdapter).payment(same(modification));
    }

    @Test
    public void testDeliveryModificationSentToViewAdapter() throws InterruptedException {
        DeliveryModification modification = new DeliveryModification(
            1, 2, 3, 4, 5L, 6, new BigDecimal("7.89")
        );
        logConsumer.addRecord(new ConsumerRecord<>(
            KafkaConfig.MODIFICATION_LOG, 0, 1, new ModificationKey(1L, (short) 1), modification
        ));

        customerStoreConsumerReadyFuture.set(null);
        lva.start();

        Thread.sleep(30);

        verify(viewAdapter).delivery(same(modification));
    }

    @Test
    public void testViewRegisteredWhenThresholdReached() throws InterruptedException {
        final RemoteStockModification modification = new RemoteStockModification(1, 2, 3);

        customerStoreConsumerReadyFuture.set(null);
        when(viewAdapter.register(0)).thenReturn(true);
        lva.start();

        // Mock consumer doesn't go through the rebalance flow, so do it manually
        lva.onPartitionsRevoked(List.of());
        lva.onPartitionsAssigned(List.of(topicPartition));

        // Add the first message
        logConsumer.schedulePollTask(() -> {
            logConsumer.addRecord(new ConsumerRecord<>(
                KafkaConfig.MODIFICATION_LOG, 0, 1, new ModificationKey(1L, (short) 1), modification
            ));
        });

        Thread.sleep(50);
        // Make sure register hasn't been called yet (nb it's called async)
        verify(viewAdapter, never()).register(0);

        // Add the second message
        logConsumer.schedulePollTask(() -> {
            logConsumer.addRecord(new ConsumerRecord<>(
                KafkaConfig.MODIFICATION_LOG, 0, 2, new ModificationKey(1L, (short) 2), modification
            ));
        });

        Thread.sleep(50);
        // Now register should have been called
        verify(viewAdapter).register(0);
        reset(viewAdapter);

        // Make sure it only gets called once
        logConsumer.schedulePollTask(() -> {
            logConsumer.addRecord(new ConsumerRecord<>(
                KafkaConfig.MODIFICATION_LOG, 0, 3, new ModificationKey(1L, (short) 3), modification
            ));
        });

        Thread.sleep(50);
        verify(viewAdapter, never()).register(0);
    }

    @Test
    public void testViewRegisteredWhenAssignedEmptyPartition() throws InterruptedException {
        customerStoreConsumerReadyFuture.set(null);
        logConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
        when(viewAdapter.register(0)).thenReturn(true);
        lva.start();

        // Mock consumer doesn't go through the rebalance flow, so do it manually
        lva.onPartitionsRevoked(List.of());
        lva.onPartitionsAssigned(List.of(topicPartition));

        Thread.sleep(20);

        verify(viewAdapter).register(0);
    }

    @Test
    public void testViewUnregisteredEvenIfNotYetRegistered() throws InterruptedException {
        customerStoreConsumerReadyFuture.set(null);
        lva.start();

        lva.onPartitionsRevoked(List.of());
        lva.onPartitionsAssigned(List.of(topicPartition));

        lva.onPartitionsRevoked(List.of(topicPartition));
        lva.onPartitionsAssigned(List.of());

        Thread.sleep(20);

        verify(viewAdapter, never()).register(anyInt());
        verify(viewAdapter).unregister(anyInt());
    }

    @Test
    public void testViewUnregisteredIfUnassignedAfterRegistered() throws InterruptedException {
        customerStoreConsumerReadyFuture.set(null);
        logConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
        when(viewAdapter.register(0)).thenReturn(true);
        lva.start();

        lva.onPartitionsRevoked(List.of());
        lva.onPartitionsAssigned(List.of(topicPartition));

        lva.onPartitionsRevoked(List.of(topicPartition));
        lva.onPartitionsAssigned(List.of());

        Thread.sleep(20);

        verify(viewAdapter).register(0);
        verify(viewAdapter).unregister(0);
    }

    @Test
    public void testReadyFutureCompletesWhenAllPartitionsReady() throws InterruptedException, ExecutionException {
        final RemoteStockModification modification = new RemoteStockModification(1, 2, 3);

        final AtomicReference<Boolean> preRegister = new AtomicReference<>();

        customerStoreConsumerReadyFuture.set(null);
        when(viewAdapter.register(0)).thenReturn(true);
        logConsumer.updateEndOffsets(Map.of(topicPartition, 0L, secondaryTopicPartition, 2L));
        lva.start();

        lva.onPartitionsRevoked(List.of());
        lva.onPartitionsAssigned(List.of(topicPartition, secondaryTopicPartition));

        Thread.sleep(20);

        when(viewAdapter.register(1)).then(invocation -> {
            preRegister.set(lva.getReadyFuture().isDone());
            return true;
        });

        logConsumer.schedulePollTask(() -> {
            logConsumer.addRecord(new ConsumerRecord<>(
                KafkaConfig.MODIFICATION_LOG, 1, 1, new ModificationKey(1L, (short) 1), modification
            ));
        });

        Thread.sleep(50);

        assertEquals(Boolean.FALSE, preRegister.get());
        verify(viewAdapter).register(0);
        assertTrue(lva.getReadyFuture().isDone());
        lva.getReadyFuture().get();
    }

    @Test
    public void testDuplicateMessagesDiscarded() throws InterruptedException {
        final RemoteStockModification modificationA = new RemoteStockModification(1, 2, 3);
        final RemoteStockModification modificationB = new RemoteStockModification(4, 5, 6);
        logConsumer.addRecord(new ConsumerRecord<>(
            KafkaConfig.MODIFICATION_LOG, 0, 1, new ModificationKey(1L, (short) 1), modificationA
        ));
        logConsumer.addRecord(new ConsumerRecord<>(
            KafkaConfig.MODIFICATION_LOG, 0, 2, new ModificationKey(1L, (short) 1), modificationB
        ));

        customerStoreConsumerReadyFuture.set(null);
        lva.start();

        Thread.sleep(50);

        // Should only have been given the first copy
        verify(viewAdapter).remoteStock(same(modificationA));
    }

    @Test
    public void testCloseShutsDownThread() throws InterruptedException {
        customerStoreConsumerReadyFuture.set(null);
        lva.start();

        lva.close();

        verify(logConsumer).close();
        verify(viewAdapter).close();
        verify(customerStoreConsumer).close();
        assertFalse(lva.isAlive());
    }
}
