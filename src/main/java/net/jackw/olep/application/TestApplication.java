package net.jackw.olep.application;

import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestApplication {
    private static
    final CountDownLatch complete = new CountDownLatch(12);

    public static void main(String[] args) throws InterruptedException {
        try (Database connection = new Database("localhost:9092", "localhost")) {
            RandomDataGenerator rand = new RandomDataGenerator();

            for (int i = 0; i < 3; i++) {
                final int itemId = rand.nextInt(20);
                NewOrderRequest.OrderLine line = new NewOrderRequest.OrderLine(itemId, 1, 5);
                connection.newOrder(
                    10, 1, 1, List.of(line, line, line, line, line, line, line, line, line, line, line)
                ).register(new StatusPrinter<>("New-Order"));

                connection.payment(
                    10, 1, 1, 1, 1, new BigDecimal("31.20")
                ).register(new StatusPrinter<>("Payment"));

                connection.payment(
                    CommonFieldGenerators.generateLastName(rand.uniform(0, 19)), 1, 6,
                    1, 3, new BigDecimal("31.20")
                ).register(new StatusPrinter<>("Payment+name"));

                connection.delivery(1, rand.nextInt(500)).register(new StatusPrinter<>("delivery"));

            }

            complete.await();
            System.out.printf("%d items are below the stock threshold\n", connection.stockLevel(1, 1, 80));
        }
    }

    private static class StatusPrinter<T extends TransactionResultMessage> implements TransactionStatusListener<T> {
        private String type;

        public StatusPrinter(String type) {
            this.type = type;
        }

        @Override
        public void acceptedHandler() {
            System.out.printf("%s transaction accepted\n", type);
        }

        @Override
        public void rejectedHandler(Throwable t) {
            System.out.printf("%s transaction rejected\n", type);
            complete.countDown();
        }

        @Override
        public void completeHandler(T result) {
            System.out.println(result.toString());
            complete.countDown();
        }
    }
}
