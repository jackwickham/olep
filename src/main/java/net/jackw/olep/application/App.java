package net.jackw.olep.application;

import akka.actor.ActorSystem;
import net.jackw.olep.common.DatabaseConfig;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        DatabaseConfig config = DatabaseConfig.create(args);

        ActorSystem system = ActorSystem.create("olep");

        start(config, system);
    }

    public static void start(DatabaseConfig config, ActorSystem system) {
        for (int i = 1; i <= config.getWarehouseCount(); i += 200) {
            int range = Math.min(200, config.getWarehouseCount() - (i-1));
            system.actorOf(TerminalGroup.props(i, range, config), "term-group-" + i);
        }
    }
}
