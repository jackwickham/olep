package net.jackw.olep.application;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import com.codahale.metrics.MetricRegistry;
import net.jackw.olep.application.transaction.DeliveryDispatcher;
import net.jackw.olep.application.transaction.NewOrderDispatcher;
import net.jackw.olep.application.transaction.OrderStatusDispatcher;
import net.jackw.olep.application.transaction.PaymentDispatcher;
import net.jackw.olep.application.transaction.StockLevelDispatcher;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.utils.RandomDataGenerator;

import java.time.Duration;

public class Terminal extends AbstractActorWithTimers {
    private static final Object NEXT_TRANSACTION_TIMER_KEY = "NextTransactionTimer";

    private final RandomDataGenerator rand;

    private final NewOrderDispatcher newOrderDispatcher;
    private final PaymentDispatcher paymentDispatcher;
    private final DeliveryDispatcher deliveryDispatcher;
    private final OrderStatusDispatcher orderStatusDispatcher;
    private final StockLevelDispatcher stockLevelDispatcher;

    public Terminal(int warehouseId, int districtId, Database db, DatabaseConfig config, MetricRegistry registry) {
        rand = new RandomDataGenerator();

        newOrderDispatcher = new NewOrderDispatcher(warehouseId, getSelf(), getContext().getSystem(), db, rand, config, registry);
        paymentDispatcher = new PaymentDispatcher(warehouseId, getSelf(), getContext().getSystem(), db, rand, config, registry);
        deliveryDispatcher = new DeliveryDispatcher(warehouseId, getSelf(), getContext().getSystem(), db, rand, config, registry);
        orderStatusDispatcher = new OrderStatusDispatcher(warehouseId, getSelf(), getContext().dispatcher(), db, rand, config, registry);
        stockLevelDispatcher = new StockLevelDispatcher(warehouseId, districtId, getSelf(), getContext().dispatcher(), db, rand, config, registry);
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
                throw new RuntimeException("Violation: failed to receive response to " + msg + " in time");
            })
            .match(IllegalTransactionResponseException.class, e -> {
                throw e;
            })
            .build();
    }

    public static Props props(int warehouseId, int districtId, Database db, DatabaseConfig config, MetricRegistry registry) {
        return Props.create(Terminal.class, () -> new Terminal(warehouseId, districtId, db, config, registry));
    }

    @Override
    public void preStart() {
        nextTransaction();
    }

    private void nextTransaction() {
        // Choose which transaction should be sent next, according to TPC-C §5.2.3
        TransactionType event;
        float n = rand.nextFloat() * 100;
        if (n < 4.2) {
            event = TransactionType.STOCK_LEVEL;
        } else if (n < 8.4) {
            event = TransactionType.ORDER_STATUS;
        } else if (n < 12.6) {
            event = TransactionType.DELIVERY;
        } else if (n < 55.8) {
            event = TransactionType.PAYMENT;
        } else {
            event = TransactionType.NEW_ORDER;
        }
        // Compute the time until that transaction should be sent, according to TPC-C §5.2.5.4
        double thinkTime = -Math.log(rand.nextDouble()) * event.thinkTime;
        double totalDelay = thinkTime + event.keyingTime;

        getTimers().startSingleTimer(NEXT_TRANSACTION_TIMER_KEY, event, Duration.ofMillis((long)(totalDelay * 1000.0)));
    }
}
