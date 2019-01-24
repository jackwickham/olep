package net.jackw.olep.application;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
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

    public Terminal(int warehouseId, int districtId, Database db) {
        this.warehouseId = warehouseId;
        this.primaryDistrictId = districtId;
        this.db = db;
        rand = new RandomDataGenerator();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(TransactionCompleteMessage.class, msg -> {
                System.out.printf("Received message %s\n", msg);
                getTimers().cancel(TRANSACTION_TIMEOUT_TIMER_KEY);
                nextTransaction();
            })
            .matchEquals(TransactionType.NEW_ORDER, _msg -> performNewOrder())
            .matchEquals(TransactionType.PAYMENT, _msg -> performPayment())
            .matchEquals(TransactionType.ORDER_STATUS, _msg -> performOrderStatus())
            .matchEquals(TransactionType.DELIVERY, _msg -> performDelivery())
            .matchEquals(TransactionType.STOCK_LEVEL, _msg -> performDelivery())
            .match(InProgressTransaction.class, msg -> {
                throw new RuntimeException("Violation: failed to receive response to " + msg + " in time");
            })
            .build();
    }

    public static Props props(int warehouseId, int districtId, Database db) {
        return Props.create(Terminal.class, () -> new Terminal(warehouseId, districtId, db));
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
     * Perform a New-Order transaction, generating data as specified by TPC-C §2.4.1
     */
    private void performNewOrder() {
        System.out.println("Performing a new order");
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

        TransactionStatus<NewOrderResult> status = db.newOrder(customerId, districtId, warehouseId, linesBuilder.build());
        status.register(new AkkaTransactionStatusListener<>(getSelf()));

        startTimeoutTimer(new InProgressTransaction(status.getTransactionId(), TransactionType.NEW_ORDER));
    }

    /**
     * Perform a Payment transaction, as specified in TPC-C §2.5.1
     */
    private void performPayment() {
        System.out.println("Performing a payment");
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

        startTimeoutTimer(new InProgressTransaction(status.getTransactionId(), TransactionType.PAYMENT));
    }

    private void performOrderStatus() {
        System.out.println("Performing order status");
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
        System.out.println("Performing delivery");
        // The carrier number is randomly selected within [1 .. 10]
        int carrierNumber = rand.uniform(1, 10);

        TransactionStatus<DeliveryResult> status = db.delivery(warehouseId, carrierNumber);
        status.register(new AkkaTransactionStatusListener<>(getSelf()));

        startTimeoutTimer(new InProgressTransaction(status.getTransactionId(), TransactionType.DELIVERY));
    }

    private void performStockLevel() {
        System.out.println("Performing stock level");
        // The threshold of minimum quantity in stock is selected at random within [10 .. 20]
        int threshold = rand.uniform(10, 20);

        int result = db.stockLevel(primaryDistrictId, warehouseId, threshold);

        // Synchronous, so trigger the next transaction
        nextTransaction();
    }

    private void startTimeoutTimer(InProgressTransaction tx) {
        getTimers().startSingleTimer(TRANSACTION_TIMEOUT_TIMER_KEY, tx, Duration.ofSeconds(5));
    }
}
