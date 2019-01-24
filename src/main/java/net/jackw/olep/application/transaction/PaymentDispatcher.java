package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;

/**
 * Perform a Payment transaction, as specified in TPC-C §2.5.1
 */
public class PaymentDispatcher {
    private final int warehouseId;
    private final ActorRef actor;
    private final ActorSystem actorSystem;
    private final Database db;
    private final RandomDataGenerator rand;

    private final Timer acceptedTimer;
    private final Timer completeTimer;

    public PaymentDispatcher(
        int warehouseId, ActorRef actor, ActorSystem actorSystem, Database db, RandomDataGenerator rand,
        MetricRegistry registry
    ) {
        this.warehouseId = warehouseId;
        this.actor = actor;
        this.actorSystem = actorSystem;
        this.db = db;
        this.rand = rand;

        acceptedTimer = registry.timer(MetricRegistry.name(PaymentDispatcher.class, "accepted"));
        completeTimer = registry.timer(MetricRegistry.name(PaymentDispatcher.class, "success"));
    }

    public void dispatch() {
        // The district number (D_ID) is randomly selected within [1 .. 10]
        int districtId = rand.uniform(1, KafkaConfig.districtsPerWarehouse());
        // The customer resident warehouse is the home warehouse 85% of the time, and is a randomly selected remote
        // warehouse 15% of the time.
        boolean remote = rand.choice(15);
        int customerWarehouseId;
        int customerDistrictId;
        if (remote) {
            do {
                customerWarehouseId = rand.uniform(1, KafkaConfig.warehouseCount());
            } while (customerWarehouseId == warehouseId);
            customerDistrictId = rand.uniform(1, 10);
        } else {
            customerWarehouseId = warehouseId;
            customerDistrictId = districtId;
        }
        // The payment amount is randomly selected within [1.00 .. 5,000.00]
        BigDecimal amount = rand.uniform(100, 5000000, 2);

        TransactionStatus<PaymentResult> status;
        ResultHandler handler = new ResultHandler();

        // The customer is randomly selected 60% of the time by last name, and 40% of the time by number
        boolean selectByName = rand.choice(60);
        if (selectByName) {
            String name = CommonFieldGenerators.generateLastName(rand.nuRand(255, 0, KafkaConfig.customerNameRange() - 1));
            status = db.payment(name, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount);
        } else {
            int id = rand.nuRand(1023, 1, KafkaConfig.customersPerDistrict());
            status = db.payment(id, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount);
        }

        handler.attach(status);
    }

    private class ResultHandler extends BaseResultHandler<PaymentResult> {
        private final Timer.Context acceptedTimerContext;
        private final Timer.Context completeTimerContext;

        public ResultHandler() {
            super(actorSystem, actor);

            acceptedTimerContext = acceptedTimer.time();
            completeTimerContext = completeTimer.time();
        }

        @Override
        public void acceptedHandler() {
            acceptedTimerContext.stop();
        }

        @Override
        public void completeHandler(PaymentResult result) {
            completeTimerContext.stop();
            done(new TransactionCompleteMessage());
        }
    }
}
