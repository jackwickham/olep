package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.dispatch.Futures;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionTimeoutMessage;
import net.jackw.olep.application.TransactionType;
import net.jackw.olep.edge.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.time.Duration;
import java.util.concurrent.Callable;

public class OrderStatusDispatcher {
    private final int warehouseId;
    private final ActorRef actor;
    private final ActorSystem actorSystem;
    private final ExecutionContext executionContext;
    private final Database db;
    private final RandomDataGenerator rand;
    private final DatabaseConfig config;
    private final Metrics metricsManager;

    public OrderStatusDispatcher(
        int warehouseId, ActorRef actor, ActorSystem actorSystem, ExecutionContext executionContext, Database db,
        RandomDataGenerator rand, DatabaseConfig config
    ) {
        this.warehouseId = warehouseId;
        this.actor = actor;
        this.actorSystem = actorSystem;
        this.executionContext = executionContext;
        this.db = db;
        this.rand = rand;
        this.config = config;
        this.metricsManager = config.getMetrics();
    }

    public void dispatch() {
        TransactionTimeoutMessage timeoutMessage = new TransactionTimeoutMessage(TransactionType.STOCK_LEVEL);
        Cancellable scheduledTimeoutMessage = actorSystem.scheduler().scheduleOnce(
            Duration.ofSeconds(90), actor, timeoutMessage, executionContext, ActorRef.noSender()
        );

        // The district number is randomly selected within [1 .. 10]
        int districtId = rand.uniform(1, config.getDistrictsPerWarehouse());

        Future<OrderStatusResult> result;

        // The customer is randomly selected 60% of the time by last name and 40% of the time by number
        boolean selectByName = rand.choice(60);
        if (selectByName) {
            String name = CommonFieldGenerators.generateLastName(rand.nuRand(255, 0, config.getCustomerNameRange() - 1));
            result = timeTransactionAsync(() -> db.orderStatus(name, districtId, warehouseId));
        } else {
            int customerId = rand.nuRand(1023, 1, config.getCustomersPerDistrict());
            result = timeTransactionAsync(() -> db.orderStatus(customerId, districtId, warehouseId));
        }

        result.onComplete((resultOpt) -> {
            // The result can be checked here
            scheduledTimeoutMessage.cancel();
            actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());
            return null;
        }, executionContext);

    }

    private Future<OrderStatusResult> timeTransactionAsync(Callable<OrderStatusResult> transaction) {
        return Futures.future(() -> {
            Timer timer = metricsManager.startTimer();
            OrderStatusResult res = transaction.call();
            metricsManager.recordDuration(DurationType.ORDER_STATUS_COMPLETE, timer);
            return res;
        }, executionContext);
    }
}
