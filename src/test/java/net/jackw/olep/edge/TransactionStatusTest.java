package net.jackw.olep.edge;

import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.verification.VerificationMode;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class TransactionStatusTest {
    private CompletableFuture<Void> transactionDeliveredFuture = new CompletableFuture<>();
    private CompletableFuture<Void> transactionAcceptedFuture = new CompletableFuture<>();
    private CompletableFuture<TransactionResultMessage> transactionCompleteFuture = new CompletableFuture<>();

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

        transactionAcceptedFuture.complete(null);

        assertHandlersCalled(transactionStatus, HANDLER_NONE);
    }

    @Test public void testNoHandlersCalledWhenOnlyCompleteFutureCompletes() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionCompleteFuture.complete(mock(TransactionResultMessage.class));

        assertHandlersCalled(transactionStatus, HANDLER_NONE);
    }

    @Test public void testOnlyDeliveryHandlerCalledWhenDeliveryFutureCompletes() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.complete(null);

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED);
    }

    @Test public void testDeliveryAndAcceptedHandlerCalledWhenDeliveryAndAcceptedFuturesComplete() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.complete(null);
        transactionAcceptedFuture.complete(null);

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_ACCEPTED);
    }

    @Test public void testAllSuccessHandlerCalledWhenAllSuccessFuturesComplete() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.complete(null);
        transactionAcceptedFuture.complete(null);
        transactionCompleteFuture.complete(mock(TransactionResultMessage.class));

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_ACCEPTED | HANDLER_COMPLETED);
    }

    @Test public void testDeliveryFailedAndRejectedHandlersCalledWhenDeliveryFutureFails() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.completeExceptionally(new Exception());

        assertHandlersCalled(transactionStatus, HANDLER_UNDELIVERED | HANDLER_REJECTED);
    }

    @Test public void testRejectedHandlerCalledWhenAcceptedFutureFails() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.complete(null);
        transactionAcceptedFuture.completeExceptionally(new Exception());

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_REJECTED);
    }

    @Test public void testDeliveryHandlerCalledOnCompletionIfAttachedBefore() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Runnable deliveredHandler = mock(Runnable.class);
        transactionStatus.addDeliveredHandler(deliveredHandler);

        transactionDeliveredFuture.complete(null);

        verify(deliveredHandler, times(1)).run();
    }

    @Test public void testDeliveryFailedHandlerCalledWithExceptionThatItWasRejectedWith() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Exception e = new Exception();
        Consumer<Throwable> deliveredFailedHandler = mock(Consumer.class);
        transactionStatus.addDeliveryFailedHandler(deliveredFailedHandler);

        transactionDeliveredFuture.completeExceptionally(e);

        verify(deliveredFailedHandler).accept(e);
    }

    @Test public void testRejectedHandlerCalledWithDeliveryFailedException() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Exception e = new Exception();
        Consumer<Throwable> rejectedHandler = mock(Consumer.class);
        transactionStatus.addRejectedHandler(rejectedHandler);

        transactionDeliveredFuture.completeExceptionally(e);

        verify(rejectedHandler).accept(e);
    }

    @Test public void testRejectedHandlerCalledWitTransactionRejectedException() {
        TransactionStatus<TransactionResultMessage> transactionStatus = new TransactionStatus<>(1, transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Exception e = new Exception();
        Consumer<Throwable> rejectedHandler = mock(Consumer.class);
        transactionStatus.addRejectedHandler(rejectedHandler);

        transactionDeliveredFuture.complete(null);
        transactionAcceptedFuture.completeExceptionally(e);

        verify(rejectedHandler).accept(e);
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
}
