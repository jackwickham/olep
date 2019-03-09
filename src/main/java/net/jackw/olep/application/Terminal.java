package net.jackw.olep.application;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import net.jackw.olep.application.transaction.DeliveryDispatcher;
import net.jackw.olep.application.transaction.NewOrderDispatcher;
import net.jackw.olep.application.transaction.OrderStatusDispatcher;
import net.jackw.olep.application.transaction.PaymentDispatcher;
import net.jackw.olep.application.transaction.StockLevelDispatcher;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.metrics.EventType;
import net.jackw.olep.utils.RandomDataGenerator;

import java.time.Duration;

public class Terminal extends AbstractActorWithTimers {
    private static final Object NEXT_TRANSACTION_TIMER_KEY = "NextTransactionTimer";

    private final RandomDataGenerator rand;
    private final DatabaseConfig config;

    private final NewOrderDispatcher newOrderDispatcher;
    private final PaymentDispatcher paymentDispatcher;
    private final DeliveryDispatcher deliveryDispatcher;
    private final OrderStatusDispatcher orderStatusDispatcher;
    private final StockLevelDispatcher stockLevelDispatcher;

    public Terminal(int warehouseId, int districtId, Database db, DatabaseConfig config) {
        rand = new RandomDataGenerator();
        this.config = config;

        newOrderDispatcher = new NewOrderDispatcher(warehouseId, getSelf(), getContext().getSystem(), db, rand, config);
        paymentDispatcher = new PaymentDispatcher(warehouseId, getSelf(), getContext().getSystem(), db, rand, config);
        deliveryDispatcher = new DeliveryDispatcher(warehouseId, getSelf(), getContext().getSystem(), db, rand, config);
        orderStatusDispatcher = new OrderStatusDispatcher(
            warehouseId, getSelf(), getContext().getSystem(), getContext().getDispatcher(), db, rand, config
        );
        stockLevelDispatcher = new StockLevelDispatcher(
            warehouseId, districtId, getSelf(), getContext().getSystem(), getContext().getDispatcher(), db, rand, config
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(TransactionCompleteMessage.class, msg -> {
                nextTransaction();
            })
            .matchEquals(TransactionType.NEW_ORDER, _msg -> newOrderDispatcher.dispatch())
            .matchEquals(TransactionType.PAYMENT, _msg -> paymentDispatcher.dispatch())
            .matchEquals(TransactionType.ORDER_STATUS, _msg -> orderStatusDispatcher.dispatch())
            .matchEquals(TransactionType.DELIVERY, _msg -> deliveryDispatcher.dispatch())
            .matchEquals(TransactionType.STOCK_LEVEL, _msg -> stockLevelDispatcher.dispatch())
            .match(TransactionTimeoutMessage.class, msg -> {
                config.getMetrics().recordEvent(EventType.TRANSACTION_TIMEOUT, msg.toString());
                throw new TransactionTimeoutException("Violation: failed to receive response to " + msg + " in time");
            })
            .match(IllegalTransactionResponseException.class, e -> {
                throw e;
            })
            .build();
    }

    public static Props props(int warehouseId, int districtId, Database db, DatabaseConfig config) {
        return Props.create(Terminal.class, () -> new Terminal(warehouseId, districtId, db, config));
    }

    @Override
    public void preStart() {
        TransactionType type = chooseTransaction();
        // The weighted average of the think+keying times gives an average time of 20s per transaction of waiting, so
        // distribute the initial delay uniformly over 20s to get a consistent transaction rate
        float delay = rand.nextFloat() * 20;

        getTimers().startSingleTimer(NEXT_TRANSACTION_TIMER_KEY, type, Duration.ofMillis((long)(delay * 1000)));
    }

    private void nextTransaction() {
        TransactionType type = chooseTransaction();

        // Compute the time until that transaction should be sent, according to TPC-C §5.2.5.4
        double thinkTime = -Math.log(rand.nextDouble()) * type.thinkTime;
        double totalDelay = thinkTime + type.keyingTime;

        getTimers().startSingleTimer(NEXT_TRANSACTION_TIMER_KEY, type, Duration.ofMillis((long)(totalDelay * 1000.0)));
    }

    /**
     * Choose which transaction should be sent next, according to TPC-C §5.2.3
     */
    private TransactionType chooseTransaction() {
        float n = rand.nextFloat() * 100;
        if (n < 4.2) {
            return TransactionType.STOCK_LEVEL;
        } else if (n < 8.4) {
            return TransactionType.ORDER_STATUS;
        } else if (n < 12.6) {
            return TransactionType.DELIVERY;
        } else if (n < 55.8) {
            return TransactionType.PAYMENT;
        } else {
            return TransactionType.NEW_ORDER;
        }
    }
}
