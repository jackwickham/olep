package net.jackw.olep.edge;

import net.jackw.olep.edge.transaction_result.TransactionResult;
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
    private CompletableFuture<TransactionResult> transactionCompleteFuture = new CompletableFuture<>();

    private final int HANDLER_NONE = 0;
    private final int HANDLER_DELIVERED = 1;
    private final int HANDLER_ACCEPTED = 2;
    private final int HANDLER_COMPLETED = 4;
    private final int HANDLER_UNDELIVERED = 8;
    private final int HANDLER_REJECTED = 16;

    @Test public void testNoHandlersCalledWhenNoFuturesComplete() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        assertHandlersCalled(transactionStatus, HANDLER_NONE);
    }

    @Test public void testNoHandlersCalledWhenOnlyAcceptedFutureCompletes() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionAcceptedFuture.complete(null);

        assertHandlersCalled(transactionStatus, HANDLER_NONE);
    }

    @Test public void testNoHandlersCalledWhenOnlyCompleteFutureCompletes() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionCompleteFuture.complete(mock(TransactionResult.class));

        assertHandlersCalled(transactionStatus, HANDLER_NONE);
    }

    @Test public void testOnlyDeliveryHandlerCalledWhenDeliveryFutureCompletes() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.complete(null);

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED);
    }

    @Test public void testDeliveryAndAcceptedHandlerCalledWhenDeliveryAndAcceptedFuturesComplete() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.complete(null);
        transactionAcceptedFuture.complete(null);

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_ACCEPTED);
    }

    @Test public void testAllSuccessHandlerCalledWhenAllSuccessFuturesComplete() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.complete(null);
        transactionAcceptedFuture.complete(null);
        transactionCompleteFuture.complete(mock(TransactionResult.class));

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_ACCEPTED | HANDLER_COMPLETED);
    }

    @Test public void testDeliveryFailedAndRejectedHandlersCalledWhenDeliveryFutureFails() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.completeExceptionally(new Exception());

        assertHandlersCalled(transactionStatus, HANDLER_UNDELIVERED | HANDLER_REJECTED);
    }

    @Test public void testRejectedHandlerCalledWhenAcceptedFutureFails() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        transactionDeliveredFuture.complete(null);
        transactionAcceptedFuture.completeExceptionally(new Exception());

        assertHandlersCalled(transactionStatus, HANDLER_DELIVERED | HANDLER_REJECTED);
    }

    @Test public void testDeliveryHandlerCalledOnCompletionIfAttachedBefore() {
        TransactionStatus<TransactionResult> transactionStatus = new TransactionStatus<>(transactionDeliveredFuture, transactionAcceptedFuture, transactionCompleteFuture);

        Runnable deliveredHandler = mock(Runnable.class);
        transactionStatus.addDeliveredHandler(deliveredHandler);

        transactionDeliveredFuture.complete(null);

        verify(deliveredHandler, times(1)).run();
    }

    private void assertHandlersCalled(TransactionStatus<TransactionResult> transactionStatus, int handlers) {
        VerificationMode once = times(1);
        Runnable deliveredHandler = mock(Runnable.class);
        transactionStatus.addDeliveredHandler(deliveredHandler);
        verify(deliveredHandler, ((handlers & HANDLER_DELIVERED) > 0 ? once : never())).run();

        Runnable acceptedHandler = mock(Runnable.class);
        transactionStatus.addAcceptedHandler(acceptedHandler);
        verify(acceptedHandler, ((handlers & HANDLER_ACCEPTED) > 0 ? once : never())).run();

        Consumer<TransactionResult> completeHandler = mock(Consumer.class);
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
