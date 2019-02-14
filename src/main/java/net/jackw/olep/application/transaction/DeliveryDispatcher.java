package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionType;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
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
    private final DatabaseConfig config;
    private final Metrics metricsManager;

    public DeliveryDispatcher(
        int warehouseId, ActorRef actor, ActorSystem actorSystem, Database db, RandomDataGenerator rand,
        DatabaseConfig config
    ) {
        this.warehouseId = warehouseId;
        this.actor = actor;
        this.actorSystem = actorSystem;
        this.db = db;
        this.rand = rand;
        this.config = config;
        this.metricsManager = config.getMetrics();
    }

    public void dispatch() {
        // The carrier number is randomly selected within [1 .. 10]
        int carrierNumber = rand.uniform(1, 10);

        ResultHandler handler = new ResultHandler();
        TransactionStatus<DeliveryResult> status = db.delivery(warehouseId, carrierNumber);

        handler.attach(status);
    }

    private class ResultHandler extends BaseResultHandler<DeliveryResult> {
        private Timer timer;

        public ResultHandler() {
            super(actorSystem, actor, TransactionType.DELIVERY);
            timer = metricsManager.startTimer();
        }

        @Override
        public void acceptedHandler() {
            metricsManager.recordDuration(DurationType.DELIVERY_ACCEPTED, timer);
            sendMessage(new TransactionCompleteMessage());
        }

        @Override
        public void completeHandler(DeliveryResult result) {
            metricsManager.recordDuration(DurationType.DELIVERY_COMPLETE, timer);
            // Only stop the timeout once the transaction has been fully processed
            done();
        }
    }
}
