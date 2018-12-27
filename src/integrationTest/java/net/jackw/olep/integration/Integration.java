package net.jackw.olep.integration;

import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class Integration extends BaseIntegrationTest {
    @Before
    public void startListeners() {
        startVerifier();
        startWorker();
    }

    private boolean success = false;

    @Test
    public void test() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        try (Database db = new Database(getEventBoostrapServers(), getViewBootstrapServers())) {
            db.newOrder(1, 1, 1, List.of()).register(new TransactionStatusListener<>() {
                @Override
                public void rejectedHandler(Throwable t) {
                    success = false;
                    latch.countDown();
                }

                @Override
                public void completeHandler(NewOrderResult result) {
                    success = true;
                    latch.countDown();
                }
            });
            latch.await(5, TimeUnit.SECONDS);
        }
        assertTrue(success);
    }
}
