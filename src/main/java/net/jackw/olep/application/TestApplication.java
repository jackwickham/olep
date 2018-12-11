package net.jackw.olep.application;

import net.jackw.olep.edge.DatabaseConnection;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_request.NewOrderRequest;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestApplication {
    public static void main(String[] args) throws InterruptedException {
        try (DatabaseConnection connection = new DatabaseConnection("localhost:9092")) {
            Random rand = new Random();
            final AtomicInteger complete = new AtomicInteger(0);

            for (int i = 0; i < 3; i++) {
                final int itemId = rand.nextInt(200);
                connection.newOrder(
                    10,
                    1,
                    1,
                    List.of(new NewOrderRequest.OrderLine(itemId, 2, 3))
                ).register(new TransactionStatusListener<NewOrderResult>() {
                    @Override
                    public void acceptedHandler() {
                        System.out.printf("Transaction with item %d accepted\n", itemId);
                    }

                    @Override
                    public void rejectedHandler(Throwable t) {
                        System.out.printf("Transaction with item %d rejected\n", itemId);
                        complete.incrementAndGet();
                    }

                    @Override
                    public void completeHandler(NewOrderResult result) {
                        System.out.println(result.toString());
                        complete.incrementAndGet();
                    }
                });
            }

            Thread.sleep(5000);
            System.out.printf("%d transactions completed\n", complete.get());
        }
    }
}
