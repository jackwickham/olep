package net.jackw.olep.application;

import akka.actor.ActorRef;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;

public class AkkaTransactionStatusListener<T extends TransactionResultMessage> implements TransactionStatusListener<T> {
    private ActorRef actor;

    public AkkaTransactionStatusListener(ActorRef actor) {
        this.actor = actor;
    }

    @Override
    public void acceptedHandler() {

    }

    @Override
    public void rejectedHandler(Throwable t) {
        actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());
    }

    @Override
    public void completeHandler(T result) {
        actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());
    }
}
