package net.jackw.olep.application;

import akka.actor.ActorSystem;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        Arguments arguments = new Arguments(args);

        ActorSystem system = ActorSystem.create("olep");

        start(arguments.getConfig(), system);
    }

    public static void start(DatabaseConfig config, ActorSystem system) {
        for (int i = 1; i <= config.getWarehouseCount(); i += config.getWarehousesPerDatabaseConnection()) {
            int range = Math.min(config.getWarehousesPerDatabaseConnection(), config.getWarehouseCount() - (i-1));
            system.actorOf(TerminalGroup.props(i, range, config), "term-group-" + i);
        }
    }
}
