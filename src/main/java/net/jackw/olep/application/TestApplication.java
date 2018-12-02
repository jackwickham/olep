package net.jackw.olep.application;

import net.jackw.olep.edge.DatabaseConnection;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.edge.transaction_result.TestResult;

import java.util.Random;

public class TestApplication {
    public static void main(String[] args) throws InterruptedException {
        try (DatabaseConnection connection = new DatabaseConnection("localhost:9092")) {
            Random rand = new Random();

            for (int i = 0; i < 3; i++) {
                final int itemId = rand.nextInt(200);
                connection.test(
                    "Message " + i,
                    itemId
                ).register(new TransactionStatusListener<TestResult>() {
                    @Override
                    public void acceptedHandler() {
                        System.out.printf("Transaction with item %d accepted\n", itemId);
                    }

                    @Override
                    public void rejectedHandler(Throwable t) {
                        System.out.printf("Transaction with item %d rejected\n", itemId);
                    }

                    @Override
                    public void completeHandler(TestResult result) {
                        System.out.printf("Test result received with rand=%d and hello=%s\n", result.rnd, result.hello);
                    }
                });
            }

            Thread.sleep(5000);
        }
    }
}
