package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.dispatch.Futures;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionTimeoutMessage;
import net.jackw.olep.application.TransactionType;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
import net.jackw.olep.utils.RandomDataGenerator;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.time.Duration;

public class StockLevelDispatcher {
    private final int warehouseId;
    private final int districtId;
    private final ActorRef actor;
    private final ActorSystem actorSystem;
    private final ExecutionContext executionContext;
    private final Database db;
    private final RandomDataGenerator rand;
    private final DatabaseConfig config;
    private final Metrics metricsManager;

    public StockLevelDispatcher(
        int warehouseId, int districtId, ActorRef actor, ActorSystem actorSystem, Database db,
        RandomDataGenerator rand, DatabaseConfig config
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.actor = actor;
        this.actorSystem = actorSystem;
        this.executionContext = actorSystem.getDispatcher();
        this.db = db;
        this.rand = rand;
        this.config = config;
        this.metricsManager = config.getMetrics();
    }

    public void dispatch() {
        // The threshold of minimum quantity in stock is selected at random within [10 .. 20]
        int threshold = rand.uniform(10, 20);

        Future<Integer> result = Futures.future(() -> {
            TransactionTimeoutMessage timeoutMessage = new TransactionTimeoutMessage(TransactionType.STOCK_LEVEL);
            Cancellable scheduledTimeoutMessage = actorSystem.scheduler().scheduleOnce(
                Duration.ofSeconds(90), actor, timeoutMessage, executionContext, ActorRef.noSender()
            );

            Timer timer = metricsManager.startTimer();
            int res = db.stockLevel(districtId, warehouseId, threshold);
            metricsManager.recordDuration(DurationType.STOCK_LEVEL_COMPLETE, timer);

            scheduledTimeoutMessage.cancel();

            actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());

            return res;
        }, executionContext);

        // The result can be checked here
    }
}
