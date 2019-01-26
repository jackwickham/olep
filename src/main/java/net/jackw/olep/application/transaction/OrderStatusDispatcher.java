package net.jackw.olep.application.transaction;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.dispatch.Futures;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.concurrent.Callable;

public class OrderStatusDispatcher {
    private final int warehouseId;
    private final ActorRef actor;
    private final Database db;
    private final RandomDataGenerator rand;
    private final ExecutionContext executionContext;

    private final Timer completeTimer;

    public OrderStatusDispatcher(
        int warehouseId, ActorRef actor, ActorContext actorContext, Database db, RandomDataGenerator rand,
        MetricRegistry registry
    ) {
        this.warehouseId = warehouseId;
        this.actor = actor;
        this.db = db;
        this.rand = rand;
        this.executionContext = actorContext.dispatcher();

        completeTimer = registry.timer(MetricRegistry.name(OrderStatusDispatcher.class, "complete"));
    }

    public void dispatch() {
        // The district number is randomly selected within [1 .. 10]
        int districtId = rand.uniform(1, KafkaConfig.districtsPerWarehouse());

        Future<OrderStatusResult> result;

        // The customer is randomly selected 60% of the time by lsat name and 40% of the time by number
        boolean selectByName = rand.choice(60);
        if (selectByName) {
            String name = CommonFieldGenerators.generateLastName(rand.nuRand(255, 0, KafkaConfig.customerNameRange() - 1));
            result = timeTransactionAsync(() -> db.orderStatus(name, districtId, warehouseId));
        } else {
            int customerId = rand.nuRand(1023, 1, KafkaConfig.customersPerDistrict());
            Timer.Context completeTimerContext = completeTimer.time();
            result = timeTransactionAsync(() -> db.orderStatus(customerId, districtId, warehouseId));
            completeTimerContext.stop();
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
