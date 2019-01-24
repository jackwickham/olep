package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.edge.Database;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;

public class StockLevelDispatcher {
    private final int warehouseId;
    private final int districtId;
    private final ActorRef actor;
    private final Database db;
    private final RandomDataGenerator rand;

    private final Timer completeTimer;

    public StockLevelDispatcher(
        int warehouseId, int districtId, ActorRef actor, Database db, RandomDataGenerator rand, MetricRegistry registry
    ) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.actor = actor;
        this.db = db;
        this.rand = rand;

        completeTimer = registry.timer(MetricRegistry.name(StockLevelDispatcher.class, "complete"));
    }

    public void dispatch() {
        // The threshold of minimum quantity in stock is selected at random within [10 .. 20]
        int threshold = rand.uniform(10, 20);

        Timer.Context completeTimerContext = completeTimer.time();
        int result = db.stockLevel(districtId, warehouseId, threshold);
        completeTimerContext.stop();

        // The result can be checked here

        actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());
    }
}
