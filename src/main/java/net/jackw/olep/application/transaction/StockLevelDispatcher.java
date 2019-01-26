package net.jackw.olep.application.transaction;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.dispatch.Futures;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.utils.RandomDataGenerator;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

public class StockLevelDispatcher {
    private final int warehouseId;
    private final int districtId;
    private final ActorRef actor;
    private final Database db;
    private final RandomDataGenerator rand;
    private final ExecutionContext executionContext;

    private final Timer completeTimer;

    public StockLevelDispatcher(
        int warehouseId, int districtId, ActorRef actor, ActorContext actorContext, Database db, RandomDataGenerator rand,
        MetricRegistry registry
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.actor = actor;
        this.executionContext = actorContext.dispatcher();
        this.db = db;
        this.rand = rand;

        completeTimer = registry.timer(MetricRegistry.name(StockLevelDispatcher.class, "complete"));
    }

    public void dispatch() {
        // The threshold of minimum quantity in stock is selected at random within [10 .. 20]
        int threshold = rand.uniform(10, 20);

        Future<Integer> result = Futures.future(() -> {
            Timer.Context completeTimerContext = completeTimer.time();
            int res = db.stockLevel(districtId, warehouseId, threshold);
            completeTimerContext.stop();

            actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());

            return res;
        }, executionContext);

        // The result can be checked here
    }
}
