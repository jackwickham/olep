package net.jackw.olep.application;

import net.jackw.olep.edge.DatabaseConnection;

public class TestApplication {
    public static void main(String[] args) {
        try (DatabaseConnection connection = new DatabaseConnection("localhost:9092")) {
            for (int i = 0; i < 3; i++) {
                connection.test("Message " + i);
            }
            connection.awaitRecord();
        }
    }
}
