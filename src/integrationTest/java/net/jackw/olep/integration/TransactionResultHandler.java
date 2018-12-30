package net.jackw.olep.integration;

import net.jackw.olep.edge.TransactionRejectedException;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.StringDescription;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransactionResultHandler {
    private CountDownLatch latch = null;
    private Throwable failure = null;
    private int listeners = 0;

    /**
     * Wait for all of the attached listeners to be fulfilled
     *
     * This method throws exceptions if any of the assertions failed
     */
    public void await() throws Throwable {
        if (latch == null) {
            latch = new CountDownLatch(listeners);
        }
        if (!latch.await(60, TimeUnit.SECONDS)) {
            // Latch timed out
            throw new AssertionError(String.format("%d transaction results were not received", latch.getCount()));
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Get a new transaction status listener which is waiting for the transaction to be successful.
     *
     * The transaction result will be tested with the provided matcher.
     */
    public <T extends TransactionResultMessage> TransactionStatusListener<T> successListener(Matcher<T> matcher) {
        listeners++;
        return new TransactionSuccessListener<>(matcher);
    }

    /**
     * Get a new transaction status listener which is waiting for the transaction to be rejected
     */
    public <T extends TransactionResultMessage> TransactionStatusListener<T> rejectedListener() {
        listeners++;
        return new TransactionRejectedListener<>();
    }

    /**
     * TransactionStatusListener that verifies that the transaction completed successfully
     */
    private class TransactionSuccessListener<T extends TransactionResultMessage> implements TransactionStatusListener<T> {
        private Matcher<T> matcher;

        public TransactionSuccessListener(Matcher<T> matcher) {
            this.matcher = matcher;
        }

        @Override
        public void rejectedHandler(Throwable t) {
            reportFailure(t);
        }

        @Override
        public void completeHandler(T result) {
            try {
                MatcherAssert.assertThat("", result, matcher);
                latch.countDown();
            } catch (Throwable e) {
                reportFailure(e);
            }
        }

        private void reportFailure(Throwable t) {
            failure = t;
            while (latch.getCount() > 0) {
                latch.countDown();
            }
        }
    }

    /**
     * TransactionStatusListener that verifies that the transaction was rejected by the verifier
     */
    private class TransactionRejectedListener<T extends TransactionResultMessage> implements TransactionStatusListener<T> {
        @Override
        public void rejectedHandler(Throwable t) {
            if (t instanceof TransactionRejectedException) {
                latch.countDown();
            } else {
                reportFailure(t);
            }
        }

        @Override
        public void completeHandler(T result) {
            reportFailure(new AssertionError("Transaction should not have completed"));
        }

        private void reportFailure(Throwable t) {
            failure = t;
            while (latch.getCount() > 0) {
                latch.countDown();
            }
        }
    }
}
