package net.jackw.olep.application;

import net.jackw.olep.edge.DatabaseConnection;

import java.util.Random;

public class TestApplication {
    public static void main(String[] args) throws InterruptedException {
        try (DatabaseConnection connection = new DatabaseConnection("localhost:9092")) {
            Random rand = new Random();

            for (int i = 0; i < 3; i++) {
                connection.test("Message " + i, rand.nextInt(200)).addCompleteHandler(v -> {
                    System.out.printf("Test result received with rand=%d and hello=%s\n", v.rnd, v.hello);
                });
            }

            Thread.sleep(5000);
        }
    }
}
