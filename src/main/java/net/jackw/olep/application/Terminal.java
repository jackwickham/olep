package net.jackw.olep.application;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.jackw.olep.application.transaction.NewOrderDispatcher;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;
import java.time.Duration;

public class Terminal extends AbstractActorWithTimers {
    private static final Object NEXT_TRANSACTION_TIMER_KEY = "NextTransactionTimer";
    private static final Object TRANSACTION_TIMEOUT_TIMER_KEY = "TransactionTimeoutTimer";

    private final int warehouseId;
    private final int primaryDistrictId;
    private final Database db;
    private final RandomDataGenerator rand;

    private final NewOrderDispatcher newOrderDispatcher;

    public Terminal(int warehouseId, int districtId, Database db, MetricRegistry registry) {
        this.warehouseId = warehouseId;
        this.primaryDistrictId = districtId;
        this.db = db;
        rand = new RandomDataGenerator();

        newOrderDispatcher = new NewOrderDispatcher(warehouseId, getSelf(), getContext().getSystem(), db, rand, registry);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(TransactionCompleteMessage.class, msg -> {
                getTimers().cancel(TRANSACTION_TIMEOUT_TIMER_KEY);
                nextTransaction();
            })
            .matchEquals(TransactionType.NEW_ORDER, _msg -> newOrderDispatcher.dispatch())
            .matchEquals(TransactionType.PAYMENT, _msg -> performPayment())
            .matchEquals(TransactionType.ORDER_STATUS, _msg -> performOrderStatus())
            .matchEquals(TransactionType.DELIVERY, _msg -> performDelivery())
            .matchEquals(TransactionType.STOCK_LEVEL, _msg -> performStockLevel())
            .match(TransactionTimeoutMessage.class, msg -> {
                throw new RuntimeException("Violation: failed to receive response to " + msg + " in time");
            })
            .build();
    }

    public static Props props(int warehouseId, int districtId, Database db, MetricRegistry registry) {
        return Props.create(Terminal.class, () -> new Terminal(warehouseId, districtId, db, registry));
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

    /**
     * Perform a Payment transaction, as specified in TPC-C §2.5.1
     */
    private void performPayment() {
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

        // The customer is randomly selected 60% of the time by last name, and 40% of the time by number
        boolean selectByName = rand.choice(60);
        if (selectByName) {
            String name = CommonFieldGenerators.generateLastName(rand.nuRand(255, 0, KafkaConfig.customerNameRange() - 1));
            status = db.payment(name, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount);
        } else {
            int id = rand.nuRand(1023, 1, KafkaConfig.customersPerDistrict());
            status = db.payment(id, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount);
        }

        status.register(new AkkaTransactionStatusListener<>(getSelf()));

        startTimeoutTimer(new TransactionTimeoutMessage(status.getTransactionId(), TransactionType.PAYMENT));
    }

    private void performOrderStatus() {
        // The district number is randomly selected within [1 .. 10]
        int districtId = rand.uniform(1, KafkaConfig.districtsPerWarehouse());

        OrderStatusResult result;

        // The customer is randomly selected 60% of the time by lsat name and 40% of the time by number
        boolean selectByName = rand.choice(60);
        if (selectByName) {
            String name = CommonFieldGenerators.generateLastName(rand.nuRand(255, 0, KafkaConfig.customerNameRange() - 1));
            result = db.orderStatus(name, districtId, warehouseId);
        } else {
            int customerId = rand.nuRand(1023, 1, KafkaConfig.customersPerDistrict());
            result = db.orderStatus(customerId, districtId, warehouseId);
        }

        // This is synchronous, so just trigger the next transaction
        nextTransaction();
    }

    private void performDelivery() {
        // The carrier number is randomly selected within [1 .. 10]
        int carrierNumber = rand.uniform(1, 10);

        TransactionStatus<DeliveryResult> status = db.delivery(warehouseId, carrierNumber);
        status.register(new AkkaTransactionStatusListener<>(getSelf()));

        startTimeoutTimer(new TransactionTimeoutMessage(status.getTransactionId(), TransactionType.DELIVERY));
    }

    private void performStockLevel() {
        // The threshold of minimum quantity in stock is selected at random within [10 .. 20]
        int threshold = rand.uniform(10, 20);

        int result = db.stockLevel(primaryDistrictId, warehouseId, threshold);

        // Synchronous, so trigger the next transaction
        nextTransaction();
    }

    private void startTimeoutTimer(TransactionTimeoutMessage tx) {
        getTimers().startSingleTimer(TRANSACTION_TIMEOUT_TIMER_KEY, tx, Duration.ofSeconds(5));
    }
}
