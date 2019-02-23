package net.jackw.olep.application;

import akka.actor.ActorSystem;
import net.jackw.olep.common.Arguments;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        Arguments arguments = new Arguments(args);

        ActorSystem system = ActorSystem.create("olep");

        system.actorOf(RootActor.props(arguments.getConfig()));
    }
}
