package net.jackw.olep.application;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class App {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("olep");
        for (int i = 0; i < 50; i++) {
            ActorRef term = system.actorOf(Terminal.props(i), "term-" + i);
            term.tell(new TransactionCompleteMessage(), ActorRef.noSender());
        }
    }
}
