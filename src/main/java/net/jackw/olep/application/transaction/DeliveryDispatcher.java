package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionType;
import net.jackw.olep.common.Database;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.utils.RandomDataGenerator;

/**
 * Perform a Payment transaction, as specified in TPC-C §2.5.1
 */
public class DeliveryDispatcher {
    private final int warehouseId;
    private final ActorRef actor;
    private final ActorSystem actorSystem;
    private final Database db;
    private final RandomDataGenerator rand;

    private final Timer acceptedTimer;
    private final Timer completeTimer;

    public DeliveryDispatcher(
        int warehouseId, ActorRef actor, ActorSystem actorSystem, Database db, RandomDataGenerator rand,
        MetricRegistry registry
    ) {
        this.warehouseId = warehouseId;
        this.actor = actor;
        this.actorSystem = actorSystem;
        this.db = db;
        this.rand = rand;

        acceptedTimer = registry.timer(MetricRegistry.name(DeliveryDispatcher.class, "accepted"));
        completeTimer = registry.timer(MetricRegistry.name(DeliveryDispatcher.class, "complete"));
    }

    public void dispatch() {
        // The carrier number is randomly selected within [1 .. 10]
        int carrierNumber = rand.uniform(1, 10);

        ResultHandler handler = new ResultHandler();
        TransactionStatus<DeliveryResult> status = db.delivery(warehouseId, carrierNumber);

        handler.attach(status);
    }

    private class ResultHandler extends BaseResultHandler<DeliveryResult> {
        private final Timer.Context acceptedTimerContext;
        private final Timer.Context completeTimerContext;

        public ResultHandler() {
            super(actorSystem, actor, TransactionType.DELIVERY);

            acceptedTimerContext = acceptedTimer.time();
            completeTimerContext = completeTimer.time();
        }

        @Override
        public void acceptedHandler() {
            acceptedTimerContext.stop();
            sendMessage(new TransactionCompleteMessage());
        }

        @Override
        public void completeHandler(DeliveryResult result) {
            completeTimerContext.stop();
            // Only stop the timeout once the transaction has been fully processed
            done();
        }
    }
}
