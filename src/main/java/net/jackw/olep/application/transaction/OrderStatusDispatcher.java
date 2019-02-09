package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.dispatch.Futures;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.concurrent.Callable;

public class OrderStatusDispatcher {
    private final int warehouseId;
    private final ActorRef actor;
    private final ExecutionContext executionContext;
    private final Database db;
    private final RandomDataGenerator rand;
    private final DatabaseConfig config;

    private final Timer completeTimer;

    public OrderStatusDispatcher(
        int warehouseId, ActorRef actor, ExecutionContext executionContext, Database db, RandomDataGenerator rand,
        DatabaseConfig config, MetricRegistry registry
    ) {
        this.warehouseId = warehouseId;
        this.actor = actor;
        this.executionContext = executionContext;
        this.db = db;
        this.rand = rand;
        this.config = config;

        completeTimer = registry.timer(
            MetricRegistry.name(OrderStatusDispatcher.class, "complete"), new TimerSupplier()
        );
    }

    public void dispatch() {
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
            actor.tell(new TransactionCompleteMessage(), ActorRef.noSender());
            return null;
        }, executionContext);

    }

    private Future<OrderStatusResult> timeTransactionAsync(Callable<OrderStatusResult> transaction) {
        return Futures.future(() -> {
            Timer.Context completeTimerContext = completeTimer.time();
            OrderStatusResult res = transaction.call();
            completeTimerContext.stop();
            return res;
        }, executionContext);
    }
}
