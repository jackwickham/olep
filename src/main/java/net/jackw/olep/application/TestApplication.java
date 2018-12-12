package net.jackw.olep.application;

import net.jackw.olep.edge.DatabaseConnection;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.message.transaction_result.TransactionResult;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestApplication {
    private static
    final AtomicInteger complete = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        try (DatabaseConnection connection = new DatabaseConnection("localhost:9092")) {
            RandomDataGenerator rand = new RandomDataGenerator();

            for (int i = 0; i < 3; i++) {
                final int itemId = rand.nextInt(200);
                connection.newOrder(
                    10,
                    1,
                    1,
                    List.of(new NewOrderRequest.OrderLine(itemId, 2, 3))
                ).register(new StatusPrinter<>("New-Order"));

                connection.payment(
                    1, 1, 1, 1, 10, new BigDecimal("31.20")
                ).register(new StatusPrinter<>("Payment"));

                connection.payment(
                    6, 1, 3, 1,
                    CommonFieldGenerators.generateLastName(rand, rand.uniform(0, 999)),
                    new BigDecimal("31.20")
                ).register(new StatusPrinter<>("Payment+name"));
            }

            Thread.sleep(5000);
            System.out.printf("%d transactions completed\n", complete.get());
        }
    }

    private static class StatusPrinter<T extends TransactionResult> implements TransactionStatusListener<T> {
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
            complete.incrementAndGet();
        }

        @Override
        public void completeHandler(T result) {
            System.out.println(result.toString());
            complete.incrementAndGet();
        }
    }
}
