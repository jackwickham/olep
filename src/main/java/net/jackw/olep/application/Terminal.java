package net.jackw.olep.application;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import com.google.common.collect.ImmutableList;
import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.utils.RandomDataGenerator;

import java.time.Duration;

public class Terminal extends AbstractActorWithTimers {
    private static final Object NEXT_TRANSACTION_TIMER_KEY = "NextTransactionTimer";
    private static final Object TRANSACTION_TIMEOUT_TIMER_KEY = "TransactionTimeoutTimer";

    private int warehouseId;
    private Database db;
    private RandomDataGenerator rand;

    public Terminal(int warehouseId, Database db) {
        this.warehouseId = warehouseId;
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
            .match(TransactionType.class, ev -> {
                System.out.printf("Received transaction event %s\n", ev);
                nextTransaction();
            })
            .match(InProgressTransaction.class, msg -> {
                throw new RuntimeException("Violation: failed to receive response to " + msg + " in time");
            })
            .build();
    }

    public static Props props(int warehouseId, Database db) {
        return Props.create(Terminal.class, () -> new Terminal(warehouseId, db));
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
     *
     * TODO: some numbers need to be abstracted away so they match the values that it was populated with
     */
    private void performNewOrder() {
        System.out.println("Performing a new order");
        // The district number (D_ID) is randomly selected within [1 .. 10]
        int districtId = rand.uniform(1, 10);
        // The non-uniform random customer number (C_ID) is selected from the NURand(1023, 1, 3000) function
        int customerId = rand.nuRand(1023, 1, 100/*3000*/);
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
                itemId = rand.nuRand(8191, 1, 20/*100000*/);
            }
            // A supplying warehouse number (OL_SUPPLY_W_ID) is selected as the home warehouse 99% of the time and as a
            // remote warehouse 1% of the time
            int supplyWarehouseId;
            if (rand.choice(1)) {
                supplyWarehouseId = rand.uniform(1, 20); // TODO: Need to know how many warehouses
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

    private void startTimeoutTimer(InProgressTransaction tx) {
        getTimers().startSingleTimer(TRANSACTION_TIMEOUT_TIMER_KEY, tx, Duration.ofSeconds(5));
    }
}
