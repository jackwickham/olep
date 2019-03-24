package net.jackw.olep.edge;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.verification.VerificationMode;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class TransactionStatusTest {
    private SettableFuture<Void> transactionDeliveredFuture = SettableFuture.create();
    private SettableFuture<Void> transactionAcceptedFuture = SettableFuture.create();
    private SettableFuture<TransactionResultMessage> transactionCompleteFuture = SettableFuture.create();

    private final int HANDLER_NONE = 0;
    private final int HANDLER_DELIVERED = 1;
    private final int HANDLER_ACCEPTED = 2;
    private final int HANDLER_COMPLETED = 4;
    private final int HANDLER_UNDELIVERED = 8;
    private final int HANDLER_REJECTED = 16;

    @Test public void testNoHandlersCalledWhenNoFuturesComplete() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        assertHandlersCalled(transactionStatus, HANDLER_NONE);
    }

    @Test public void testNoHandlersCalledWhenOnlyAcceptedFutureCompletes() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionAcceptedFuture.set(null);

        assertHandlersCalled(transactionStatus, HANDLER_NONE);
    }

    @Test public void testNoHandlersCalledWhenOnlyCompleteFutureCompletes() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionCompleteFuture.set(mock(TransactionResultMessage.class));

        assertHandlersCalled(transactionStatus, HANDLER_NONE);
    }

    @Test public void testOnlyDeliveryHandlerCalledWhenDeliveryFutureCompletes() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.set(null);

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED);
    }

    @Test public void testDeliveryAndAcceptedHandlerCalledWhenDeliveryAndAcceptedFuturesComplete() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.set(null);
        transactionAcceptedFuture.set(null);

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_ACCEPTED);
    }

    @Test public void testAllSuccessHandlerCalledWhenAllSuccessFuturesComplete() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.set(null);
        transactionAcceptedFuture.set(null);
        transactionCompleteFuture.set(mock(TransactionResultMessage.class));

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_ACCEPTED | HANDLER_COMPLETED);
    }

    @Test public void testDeliveryFailedAndRejectedHandlersCalledWhenDeliveryFutureFails() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.setException(new Exception());

        assertHandlersCalled(transactionStatus, HANDLER_UNDELIVERED | HANDLER_REJECTED);
    }

    @Test public void testRejectedHandlerCalledWhenAcceptedFutureFails() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.set(null);
        transactionAcceptedFuture.setException(new Exception());

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_REJECTED);
    }

    @Test public void testDeliveryHandlerCalledOnCompletionIfAttachedBefore() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Runnable deliveredHandler = mock(Runnable.class);
        transactionStatus.addDeliveredHandler(deliveredHandler);

        transactionDeliveredFuture.set(null);

        verify(deliveredHandler, times(1)).run();
    }

    @Test public void testCompleteHandlerCalledWhenTransactionAcceptedLate() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.set(null);
        transactionCompleteFuture.set(mock(TransactionResultMessage.class));
        transactionAcceptedFuture.set(null);

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_ACCEPTED | HANDLER_COMPLETED);
    }

    @Test public void testDeliveryFailedHandlerCalledWithExceptionThatItWasRejectedWith() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Exception e = new Exception();
        Consumer<Throwable> deliveredFailedHandler = mock(Consumer.class);
        transactionStatus.addDeliveryFailedHandler(deliveredFailedHandler);

        transactionDeliveredFuture.setException(e);

        verify(deliveredFailedHandler).accept(e);
    }

    @Test public void testRejectedHandlerCalledWithDeliveryFailedException() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Exception e = new Exception();
        Consumer<Throwable> rejectedHandler = mock(Consumer.class);
        transactionStatus.addRejectedHandler(rejectedHandler);

        transactionDeliveredFuture.setException(e);

        verify(rejectedHandler).accept(e);
    }

    @Test public void testRejectedHandlerCalledWithTransactionRejectedException() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Exception e = new Exception();
        Consumer<Throwable> rejectedHandler = mock(Consumer.class);
        transactionStatus.addRejectedHandler(rejectedHandler);

        transactionDeliveredFuture.set(null);
        transactionAcceptedFuture.setException(e);

        verify(rejectedHandler).accept(e);
    }

    @Test public void testFuturesInitiallyNotDone() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        assertFalse(transactionStatus.getDeliveryFuture().isDone());
        assertFalse(transactionStatus.getAcceptedFuture().isDone());
        assertFalse(transactionStatus.getCompleteFuture().isDone());
    }

    @Test public void testNoFuturesDoneWhenAcceptedAndCompleteFuturesComplete() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionAcceptedFuture.set(null);
        transactionCompleteFuture.set(mock(TransactionResultMessage.class));

        assertFalse(transactionStatus.getDeliveryFuture().isDone());
        assertFalse(transactionStatus.getAcceptedFuture().isDone());
        assertFalse(transactionStatus.getCompleteFuture().isDone());
    }

    @Test public void testDeliveryFutureCompletesNormally() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.set(null);

        assertFutureCompletedNormally(transactionStatus.getDeliveryFuture());
        assertFalse(transactionStatus.getAcceptedFuture().isDone());
        assertFalse(transactionStatus.getCompleteFuture().isDone());
    }

    @Test public void testAllFuturesCompleteExceptionallyWhenDeliveryFails() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.setException(new Exception());

        assertFutureCompletedExceptionally(transactionStatus.getDeliveryFuture());
        assertFutureCompletedExceptionally(transactionStatus.getAcceptedFuture());
        assertFutureCompletedExceptionally(transactionStatus.getCompleteFuture());
    }

    @Test public void testAcceptedFutureCompletesNormally() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.set(null);
        transactionAcceptedFuture.set(null);

        assertFutureCompletedNormally(transactionStatus.getDeliveryFuture());
        assertFutureCompletedNormally(transactionStatus.getAcceptedFuture());
        assertFalse(transactionStatus.getCompleteFuture().isDone());
    }

    @Test public void testAcceptedAndCompleteFuturesCompleteExceptionallyWhenAcceptedFails() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionAcceptedFuture.setException(new Exception());

        assertFalse(transactionStatus.getDeliveryFuture().isDone());
        assertFutureCompletedExceptionally(transactionStatus.getAcceptedFuture());
        assertFutureCompletedExceptionally(transactionStatus.getCompleteFuture());
    }

    @Test public void testCompleteFutureCompletesNormally() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.set(null);
        transactionAcceptedFuture.set(null);
        transactionCompleteFuture.set(null);

        assertFutureCompletedNormally(transactionStatus.getDeliveryFuture());
        assertFutureCompletedNormally(transactionStatus.getAcceptedFuture());
        assertFutureCompletedNormally(transactionStatus.getCompleteFuture());
    }

    private void assertHandlersCalled(TransactionStatus<TransactionResultMessage> transactionStatus, int handlers) {
        VerificationMode once = times(1);
        Runnable deliveredHandler = mock(Runnable.class);
        transactionStatus.addDeliveredHandler(deliveredHandler);
        verify(deliveredHandler, ((handlers & HANDLER_DELIVERED) > 0 ? once : never())).run();

        Runnable acceptedHandler = mock(Runnable.class);
        transactionStatus.addAcceptedHandler(acceptedHandler);
        verify(acceptedHandler, ((handlers & HANDLER_ACCEPTED) > 0 ? once : never())).run();

        Consumer<TransactionResultMessage> completeHandler = mock(Consumer.class);
        transactionStatus.addCompleteHandler(completeHandler);
        verify(completeHandler, ((handlers & HANDLER_COMPLETED) > 0 ? once : never())).accept(any());

        Consumer<Throwable> deliveryFailedHandler = mock(Consumer.class);
        transactionStatus.addDeliveryFailedHandler(deliveryFailedHandler);
        verify(deliveryFailedHandler, ((handlers & HANDLER_UNDELIVERED) > 0 ? once : never())).accept(any());

        Consumer<Throwable> rejectedHandler = mock(Consumer.class);
        transactionStatus.addRejectedHandler(rejectedHandler);
        verify(rejectedHandler, ((handlers & HANDLER_REJECTED) > 0 ? once : never())).accept(any());
    }

    private void assertFutureCompletedNormally(ListenableFuture<?> future) {
        assertTrue(future.isDone());
        try {
            Futures.getDone(future);
        } catch (ExecutionException e) {
            fail("Future failed with error " + e.getCause().toString());
        }
    }

    private void assertFutureCompletedExceptionally(ListenableFuture<?> future) {
        assertTrue(future.isDone());
        try {
            Futures.getDone(future);
            fail("Future completed successfully");
        } catch (ExecutionException e) {
            // Good
        }
    }
}
