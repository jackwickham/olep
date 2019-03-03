package net.jackw.olep.integration;

import net.jackw.olep.edge.EventDatabase;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.integration.matchers.DeliveryResultMatcher;
import net.jackw.olep.integration.matchers.MapEntryMatcher;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

public class DeliveryTest extends BaseIntegrationTest {
    @Before
    public void startProcessors() throws Exception {
        startVerifier();
        startWorker();
        startView();
    }

    @Test
    public void testNothingMarkedAsDeliveredWhenNoPendingOrders() throws Throwable {
        try (EventDatabase db = new EventDatabase(getConfig())) {
            TransactionResultHandler resultHandler = new TransactionResultHandler();

            TransactionStatus<DeliveryResult> deliveryStatus = db.delivery(1, 4);
            deliveryStatus.register(resultHandler.successListener(new DeliveryResultMatcher(1, 4, Matchers.emptyIterable())));

            resultHandler.await();
        }
    }

    @Test
    public void testCorrectResultWhenRecordsInserted() throws Throwable {
        try (EventDatabase db = new EventDatabase(getConfig())) {
            // Populate DB
            final CountDownLatch latch = new CountDownLatch(5);
            log.info("Starting transactions");
            List<TransactionStatus<NewOrderResult>> orders = List.of(
                db.newOrder(1, 1, 1, List.of()),
                db.newOrder(1, 2, 1, List.of()),
                db.newOrder(1, 3, 1, List.of()),
                db.newOrder(1, 4, 1, List.of()),
                db.newOrder(1, 5, 1, List.of())
            );

            final int[] orderIds = new int[5];
            AtomicReference<Throwable> errorRef = new AtomicReference<>();

            for (TransactionStatus<NewOrderResult> status : orders) {
                status.addAcceptedHandler(() -> {
                    log.info("Transaction accepted");
                });
                status.addCompleteHandler(result -> {
                    log.info("Transaction complete for district " + result.districtId);
                    orderIds[result.districtId - 1] = result.orderId;
                    latch.countDown();
                });
                status.addRejectedHandler(err -> {
                    log.error("rejected");
                    errorRef.set(err);
                    latch.countDown();
                });
            }

            assertTrue(latch.await(20, TimeUnit.SECONDS));
            Throwable err = errorRef.get();
            if (err != null) {
                throw err;
            }

            TransactionResultHandler resultHandler = new TransactionResultHandler();

            TransactionStatus<DeliveryResult> deliveryStatus = db.delivery(1, 4);
            deliveryStatus.register(resultHandler.successListener(new DeliveryResultMatcher(1, 4, Matchers.containsInAnyOrder(
                new MapEntryMatcher<>(1, orderIds[0]), new MapEntryMatcher<>(2, orderIds[1]),
                new MapEntryMatcher<>(3, orderIds[2]), new MapEntryMatcher<>(4, orderIds[3]),
                new MapEntryMatcher<>(5, orderIds[4])
            ))));

            resultHandler.await();
        }
    }

    private static Logger log = LogManager.getLogger();
}
