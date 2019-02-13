package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.dispatch.Futures;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.MetricsManager;
import net.jackw.olep.metrics.Timer;
import net.jackw.olep.utils.RandomDataGenerator;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

public class StockLevelDispatcher {
    private final int warehouseId;
    private final int districtId;
    private final ActorRef actor;
    private final ExecutionContext executionContext;
    private final Database db;
    private final RandomDataGenerator rand;
    private final DatabaseConfig config;
    private final MetricsManager metricsManager;

    public StockLevelDispatcher(
        int warehouseId, int districtId, ActorRef actor, ExecutionContext executionContext, Database db, RandomDataGenerator rand,
        DatabaseConfig config
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.actor = actor;
        this.executionContext = executionContext;
        this.db = db;
        this.rand = rand;
        this.config = config;
        this.metricsManager = MetricsManager.getInstance();
    }

    public void dispatch() {
        // The threshold of minimum quantity in stock is selected at random within [10 .. 20]
        int threshold = rand.uniform(10, 20);

        Future<Integer> result = Futures.future(() -> {
            Timer timer = metricsManager.startTimer();
            int res = db.stockLevel(districtId, warehouseId, threshold);
            metricsManager.recordDuration(DurationType.STOCK_LEVEL_COMPLETE, timer);

            actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());

            return res;
        }, executionContext);

        // The result can be checked here
    }
}
