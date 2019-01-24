package net.jackw.olep.application;

import akka.actor.ActorRef;
import com.codahale.metrics.Timer;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;

public class AkkaTransactionStatusListener<T extends TransactionResultMessage> implements TransactionStatusListener<T> {
    private ActorRef actor;
    private Timer.Context timer = null;

    public AkkaTransactionStatusListener(ActorRef actor) {
        this.actor = actor;
    }

    public AkkaTransactionStatusListener(ActorRef actor, Timer.Context timer) {
        this.actor = actor;
        this.timer = timer;
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
        if (timer != null) {
            timer.stop();
        }
        actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());
    }
}
