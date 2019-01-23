package net.jackw.olep.application;

import akka.actor.ActorSystem;
import net.jackw.olep.common.KafkaConfig;

public class App {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("olep");
        for (int i = 1; i < KafkaConfig.warehouseCount(); i += 10) {
            system.actorOf(TerminalGroup.props(i, 10), "term-group-" + i);
        }
    }
}
