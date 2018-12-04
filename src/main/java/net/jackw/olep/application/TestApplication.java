package net.jackw.olep.application;

import com.google.common.base.MoreObjects;
import net.jackw.olep.edge.DatabaseConnection;
import net.jackw.olep.edge.TransactionStatusListener;
import net.jackw.olep.edge.transaction_result.NewOrderResult;
import net.jackw.olep.edge.transaction_result.TestResult;
import net.jackw.olep.message.NewOrderMessage;

import java.util.Date;
import java.util.List;
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

                connection.newOrder(
                    10,
                    1,
                    1,
                    List.of(new NewOrderMessage.OrderLine(itemId, 1, 3))
                ).register(new TransactionStatusListener<NewOrderResult>() {
                    @Override
                    public void acceptedHandler() {
                        System.out.printf("Transaction with item %d accepted\n", itemId);
                    }

                    @Override
                    public void rejectedHandler(Throwable t) {
                        System.out.printf("Transaction with item %d rejected\n", itemId);
                    }

                    @Override
                    public void completeHandler(NewOrderResult result) {
                        System.out.println(result.toString());
                    }
                });
            }

            Thread.sleep(5000);
        }
    }
}
