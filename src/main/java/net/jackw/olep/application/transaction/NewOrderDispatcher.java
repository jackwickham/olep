package net.jackw.olep.application.transaction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import net.jackw.olep.application.TransactionCompleteMessage;
import net.jackw.olep.application.TransactionTimeoutMessage;
import net.jackw.olep.application.TransactionType;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.utils.RandomDataGenerator;

import java.time.Duration;

/**
 * Perform a New-Order transaction, generating data as specified by TPC-C §2.4.1
 */
public class NewOrderDispatcher {
    private final int warehouseId;
    private final ActorRef actor;
    private final ActorSystem actorSystem;
    private final Database db;
    private final RandomDataGenerator rand;

    private final Timer acceptedTimer;
    private final Timer successTimer;
    private final Timer failureTimer;

    public NewOrderDispatcher(
        int warehouseId, ActorRef actor, ActorSystem actorSystem, Database db, RandomDataGenerator rand,
        MetricRegistry registry
    ) {
        this.warehouseId = warehouseId;
        this.actor = actor;
        this.actorSystem = actorSystem;
        this.db = db;
        this.rand = rand;

        acceptedTimer = registry.timer(MetricRegistry.name(NewOrderDispatcher.class, "accepted"));
        successTimer = registry.timer(MetricRegistry.name(NewOrderDispatcher.class, "success"));
        failureTimer = registry.timer(MetricRegistry.name(NewOrderDispatcher.class, "failure"));
    }

    public void dispatch() {
        // The district number (D_ID) is randomly selected within [1 .. 10]
        int districtId = rand.uniform(1, KafkaConfig.districtsPerWarehouse());
        // The non-uniform random customer number (C_ID) is selected from the NURand(1023, 1, 3000) function
        int customerId = rand.nuRand(1023, 1, KafkaConfig.customersPerDistrict());
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
                itemId = rand.nuRand(8191, 1, KafkaConfig.itemCount());
            }
            // A supplying warehouse number (OL_SUPPLY_W_ID) is selected as the home warehouse 99% of the time and as a
            // remote warehouse 1% of the time
            int supplyWarehouseId;
            if (rand.choice(1)) {
                do {
                    supplyWarehouseId = rand.uniform(1, KafkaConfig.warehouseCount());
                } while (supplyWarehouseId == warehouseId);
            } else {
                supplyWarehouseId = warehouseId;
            }
            // A quantity (OL_QUANTITY) is randomly selected within [1 .. 10]
            int quantity = rand.uniform(1, 10);

            linesBuilder.add(new NewOrderRequest.OrderLine(itemId, supplyWarehouseId, quantity));
        }

        ResultHandler handler = new ResultHandler();
        TransactionStatus<NewOrderResult> status = db.newOrder(customerId, districtId, warehouseId, linesBuilder.build());
        handler.attach(status);
    }

    private class ResultHandler implements TransactionStatusListener<NewOrderResult> {
        private final Timer.Context acceptedTimerContext;
        private final Timer.Context successTimerContext;
        private final Timer.Context failureTimerContext;

        private Cancellable scheduledTimeoutMessage;

        public ResultHandler() {
            acceptedTimerContext = acceptedTimer.time();
            successTimerContext = successTimer.time();
            failureTimerContext = failureTimer.time();
        }

        public void attach(TransactionStatus<NewOrderResult> status) {
            // Add a timeout, if we don't receive a message in time
            TransactionTimeoutMessage timeoutMessage = new TransactionTimeoutMessage(
                status.getTransactionId(), TransactionType.NEW_ORDER
            );
            scheduledTimeoutMessage = actorSystem.scheduler().scheduleOnce(
                Duration.ofSeconds(8), actor, timeoutMessage, null, ActorRef.noSender()
            );
            status.register(this);
        }

        @Override
        public void acceptedHandler() {
            acceptedTimerContext.stop();
        }

        @Override
        public void rejectedHandler(Throwable t) {
            failureTimerContext.stop();

            done(new TransactionCompleteMessage());
        }

        @Override
        public void completeHandler(NewOrderResult result) {
            successTimerContext.stop();

            done(new TransactionCompleteMessage());
        }

        private void done(TransactionCompleteMessage msg) {
            // Stop the timeout message from being sent
            scheduledTimeoutMessage.cancel();

            actor.tell(msg, ActorRef.noSender());
        }
    }
}
