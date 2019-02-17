package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import net.jackw.olep.application.IllegalTransactionResponseException;
import net.jackw.olep.application.TransactionTimeoutMessage;
import net.jackw.olep.application.TransactionType;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import scala.concurrent.ExecutionContext;

import java.time.Duration;

public abstract class BaseResultHandler<T extends TransactionResultMessage> implements TransactionStatusListener<T> {
    private final ActorSystem actorSystem;
    private final ActorRef actor;
    private final ExecutionContext executionContext;
    private final TransactionType transactionType;
    private Cancellable scheduledTimeoutMessage;

    public BaseResultHandler(ActorSystem actorSystem, ActorRef actor, TransactionType type) {
        this.actorSystem = actorSystem;
        this.actor = actor;
        this.executionContext = actorSystem.dispatcher();
        this.transactionType = type;
    }

    /**
     * Attach this result handler to a transaction
     *
     * @param status The transaction to attach to
     */
    public void attach(TransactionStatus<T> status) {
        // Add a timeout to raise a violation event if the event is not received within 90 seconds
        TransactionTimeoutMessage timeoutMessage = new TransactionTimeoutMessage(
            status.getTransactionId(), transactionType
        );
        scheduledTimeoutMessage = actorSystem.scheduler().scheduleOnce(
            Duration.ofSeconds(90), actor, timeoutMessage, executionContext, ActorRef.noSender()
        );
        status.register(this);
    }

    /**
     * This transaction has completed (successfully or not)
     *
     * @param msg The result message to send to the actor
     */
    protected void done(Object msg) {
        done();
        sendMessage(msg);
    }

    /**
     * This transaction has completed successfully, but no message should be sent to the actor
     */
    protected void done() {
        // Stop the timeout message from being sent
        scheduledTimeoutMessage.cancel();
    }

    /**
     * Send a message to the actor
     *
     * @param msg The message to send
     */
    protected void sendMessage(Object msg) {
        actor.tell(msg, ActorRef.noSender());
    }

    // Default the rejected handler to produce an error
    @Override
    public void rejectedHandler(Throwable t) {
        done(new IllegalTransactionResponseException("transaction rejected incorrectly"));
    }
}
