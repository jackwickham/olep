package net.jackw.olep.application;

import net.jackw.olep.edge.DatabaseConnection;

public class TestApplication {
    public static void main(String[] args) throws InterruptedException {
        try (DatabaseConnection connection = new DatabaseConnection("localhost:9092")) {
            new Thread(() -> {
                while (true) {
                    connection.awaitRecord();
                }
            }).start();

            for (int i = 0; i < 3; i++) {
                connection.test("Message " + i).addCompleteHandler(v -> {
                    System.out.printf("Test result received with rand=%d and hello=%s\n", v.rnd, v.hello);
                });
            }

            Thread.sleep(5000);
        }
    }
}
