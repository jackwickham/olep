package net.jackw.olep.edge;

import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import net.jackw.olep.message.transaction_result.TransactionResultBuilder;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PendingTransactionTest {
    @Mock private TransactionResultBuilder<TransactionResultMessage> transactionResultBuilder;

    @Test public void testConstructionProducesCorrectInitialState() {
        PendingTransaction<TransactionResultMessage, TransactionResultBuilder<TransactionResultMessage>> pendingTransaction = new PendingTransaction<>(4, transactionResultBuilder);

        verify(transactionResultBuilder, never()).build();
        assertEquals(4, pendingTransaction.getTransactionId());
        assertEquals(pendingTransaction.getTransactionResultBuilder(), transactionResultBuilder);

        // Ensure that all of the futures are not yet completed
        assertFalse(pendingTransaction.getWrittenToLogFuture().isDone());
        assertFalse(pendingTransaction.getAcceptedFuture().isDone());
        assertFalse(pendingTransaction.getCompleteFuture().isDone());
    }

    @Test public void testWrittenToDiskCallbackSuccess() throws InterruptedException, ExecutionException, TimeoutException {
        PendingTransaction<TransactionResultMessage, TransactionResultBuilder<TransactionResultMessage>> pendingTransaction = new PendingTransaction<>(4, transactionResultBuilder);
        RecordMetadata mockRecordMetadata = new RecordMetadata(null, 0, 0, 0, null, 0, 0);

        pendingTransaction.getWrittenToLogCallback().onCompletion(mockRecordMetadata, null);

        assertEquals(mockRecordMetadata, pendingTransaction.getWrittenToLogFuture().get(1, TimeUnit.MILLISECONDS));
        assertFalse(pendingTransaction.getAcceptedFuture().isDone());
        assertFalse(pendingTransaction.getCompleteFuture().isDone());
    }

    @Test public void testWrittenToDiskCallbackFailure() {
        PendingTransaction<TransactionResultMessage, TransactionResultBuilder<TransactionResultMessage>> pendingTransaction = new PendingTransaction<>(4, transactionResultBuilder);
        Exception err = new Exception();

        pendingTransaction.getWrittenToLogCallback().onCompletion(null, err);

        assertTrue(pendingTransaction.getWrittenToLogFuture().isCompletedExceptionally());
        assertFalse(pendingTransaction.getAcceptedFuture().isDone());
        assertFalse(pendingTransaction.getCompleteFuture().isDone());
    }

    @Test public void testSetAcceptedTrue() {
        PendingTransaction<TransactionResultMessage, TransactionResultBuilder<TransactionResultMessage>> pendingTransaction = new PendingTransaction<>(4, transactionResultBuilder);

        pendingTransaction.setAccepted(true);

        assertFalse(pendingTransaction.getWrittenToLogFuture().isDone());
        assertTrue(pendingTransaction.getAcceptedFuture().isDone());
        assertFalse(pendingTransaction.getCompleteFuture().isDone());

        // Make sure it was completed successfully - this will throw if not
        pendingTransaction.getAcceptedFuture().getNow(null);
    }

    @Test public void testSetAcceptedFalse() {
        PendingTransaction<TransactionResultMessage, TransactionResultBuilder<TransactionResultMessage>> pendingTransaction = new PendingTransaction<>(4, transactionResultBuilder);

        pendingTransaction.setAccepted(false);

        assertFalse(pendingTransaction.getWrittenToLogFuture().isCompletedExceptionally());
        assertTrue(pendingTransaction.getAcceptedFuture().isDone());
        assertFalse(pendingTransaction.getCompleteFuture().isDone());
    }

    @Test public void testUpdatedBuilderButCantBuild() {
        when(transactionResultBuilder.canBuild()).thenReturn(false);
        PendingTransaction<TransactionResultMessage, TransactionResultBuilder<TransactionResultMessage>> pendingTransaction = new PendingTransaction<>(4, transactionResultBuilder);

        pendingTransaction.builderUpdated();

        verify(transactionResultBuilder, never()).build();

        assertFalse(pendingTransaction.getWrittenToLogFuture().isDone());
        assertFalse(pendingTransaction.getAcceptedFuture().isDone());
        assertFalse(pendingTransaction.getCompleteFuture().isDone());
    }


    @Test public void testUpdatedBuilderCanBuild() {
        when(transactionResultBuilder.canBuild()).thenReturn(true);
        TransactionResultMessage mockResult = mock(TransactionResultMessage.class);
        when(transactionResultBuilder.build()).thenReturn(mockResult);
        PendingTransaction<TransactionResultMessage, TransactionResultBuilder<TransactionResultMessage>> pendingTransaction = new PendingTransaction<>(4, transactionResultBuilder);

        pendingTransaction.builderUpdated();

        verify(transactionResultBuilder).build();

        assertFalse(pendingTransaction.getWrittenToLogFuture().isDone());
        assertTrue(pendingTransaction.getAcceptedFuture().isDone());
        assertTrue(pendingTransaction.getCompleteFuture().isDone());

        assertEquals(mockResult, pendingTransaction.getCompleteFuture().getNow(null));
    }
}
