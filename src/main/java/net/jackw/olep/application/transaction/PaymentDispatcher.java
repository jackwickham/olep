package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionType;
import net.jackw.olep.edge.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
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
    private final DatabaseConfig config;
    private final Metrics metricsManager;

    public PaymentDispatcher(
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
        // The district number (D_ID) is randomly selected within [1 .. 10]
        int districtId = rand.uniform(1, config.getDistrictsPerWarehouse());
        // The customer resident warehouse is the home warehouse 85% of the time, and is a randomly selected remote
        // warehouse 15% of the time.
        boolean remote = rand.choice(15);
        int customerWarehouseId;
        int customerDistrictId;
        if (remote) {
            do {
                customerWarehouseId = rand.uniform(1, config.getWarehouseCount());
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
            String name = CommonFieldGenerators.generateLastName(rand.nuRand(255, 0, config.getCustomerNameRange() - 1));
            status = db.payment(name, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount);
        } else {
            int id = rand.nuRand(1023, 1, config.getCustomersPerDistrict());
            status = db.payment(id, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount);
        }

        handler.attach(status);
    }

    private class ResultHandler extends BaseResultHandler<PaymentResult> {
        private final Timer timer;

        public ResultHandler() {
            super(actorSystem, actor, TransactionType.PAYMENT);

            timer = metricsManager.startTimer();
        }

        @Override
        public void acceptedHandler() {
            metricsManager.recordDuration(DurationType.PAYMENT_ACCEPTED, timer);
        }

        @Override
        public void completeHandler(PaymentResult result) {
            metricsManager.recordDuration(DurationType.PAYMENT_COMPLETE, timer);
            done(new TransactionCompleteMessage());
        }
    }
}
