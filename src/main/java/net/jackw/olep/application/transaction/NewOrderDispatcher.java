package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.common.collect.ImmutableList;
import net.jackw.olep.application.IllegalTransactionResponseException;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionType;
import net.jackw.olep.edge.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.metrics.DurationType;
import net.jackw.olep.metrics.Metrics;
import net.jackw.olep.metrics.Timer;
import net.jackw.olep.utils.RandomDataGenerator;

/**
 * Perform a New-Order transaction, generating data as specified by TPC-C §2.4.1
 */
public class NewOrderDispatcher {
    private final int warehouseId;
    private final ActorRef actor;
    private final ActorSystem actorSystem;
    private final Database db;
    private final RandomDataGenerator rand;
    private final DatabaseConfig config;
    private final Metrics metricsManager;

    public NewOrderDispatcher(
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
        // The non-uniform random customer number (C_ID) is selected from the NURand(1023, 1, 3000) function
        int customerId = rand.nuRand(1023, 1, config.getCustomersPerDistrict());
        // The number of items in the order (ol_cnt) is randomly selected within [5 .. 15]
        int orderLineCount = rand.uniform(5, 15);
        // A fixed 1% of the New-Order transactions are chosen at random to simulate user data entry errors
        boolean rollback = rand.choice(1);

        ImmutableList.Builder<NewOrderRequest.OrderLine> linesBuilder = ImmutableList.builderWithExpectedSize(orderLineCount);
        for (int i = 0; i < orderLineCount; i++) {
            // A non-uniform random item number (OL_I_ID) is selected using the NURand(8191, 1, 100 000) function
            // If this is the last item on the order and rbk = 1, then the item number is set to an unused value
            int itemId;
            if (rollback && i == orderLineCount - 1) {
                itemId = Integer.MAX_VALUE;
            } else {
                itemId = rand.nuRand(8191, 1, config.getItemCount());
            }
            // A supplying warehouse number (OL_SUPPLY_W_ID) is selected as the home warehouse 99% of the time and as a
            // remote warehouse 1% of the time
            int supplyWarehouseId;
            if (rand.choice(1)) {
                do {
                    supplyWarehouseId = rand.uniform(1, config.getWarehouseCount());
                } while (supplyWarehouseId == warehouseId);
            } else {
                supplyWarehouseId = warehouseId;
            }
            // A quantity (OL_QUANTITY) is randomly selected within [1 .. 10]
            int quantity = rand.uniform(1, 10);

            linesBuilder.add(new NewOrderRequest.OrderLine(itemId, supplyWarehouseId, quantity));
        }

        BaseResultHandler<NewOrderResult> handler;
        if (rollback) {
            handler = new FailureResultHandler();
        } else {
            handler = new SuccessResultHandler();
        }

        TransactionStatus<NewOrderResult> status = db.newOrder(customerId, districtId, warehouseId, linesBuilder.build());
        handler.attach(status);
    }

    private class SuccessResultHandler extends BaseResultHandler<NewOrderResult> {
        private final Timer timer;

        public SuccessResultHandler() {
            super(actorSystem, actor, TransactionType.NEW_ORDER);

            timer = metricsManager.startTimer();
        }

        @Override
        public void acceptedHandler() {
            metricsManager.recordDuration(DurationType.NEW_ORDER_ACCEPTED, timer);
        }

        @Override
        public void completeHandler(NewOrderResult result) {
            metricsManager.recordDuration(DurationType.NEW_ORDER_COMPLETE, timer);

            done(new TransactionCompleteMessage());
        }
    }

    private class FailureResultHandler extends BaseResultHandler<NewOrderResult> {
        private final Timer timer;

        public FailureResultHandler() {
            super(actorSystem, actor, TransactionType.NEW_ORDER);

            timer = metricsManager.startTimer();
        }

        @Override
        public void acceptedHandler() {
            done(new IllegalTransactionResponseException("transaction accepted incorrectly"));
        }

        @Override
        public void rejectedHandler(Throwable t) {
            metricsManager.recordDuration(DurationType.NEW_ORDER_REJECTED, timer);

            done(new TransactionCompleteMessage());
        }

        @Override
        public void completeHandler(NewOrderResult result) {
            done(new IllegalTransactionResponseException("transaction completed incorrectly"));
        }
    }
}
