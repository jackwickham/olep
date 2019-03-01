package net.jackw.olep.application;

import akka.actor.AbstractActor;
import akka.actor.AllForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import net.jackw.olep.common.DatabaseConfig;

import java.util.function.Consumer;

public class RootActor extends AbstractActor {
    private DatabaseConfig config;
    private Consumer<Throwable> onFailure;

    public RootActor(DatabaseConfig config) {
        this(config, null);
    }

    public RootActor(DatabaseConfig config, Consumer<Throwable> onFailure) {
        this.config = config;
        this.onFailure = onFailure;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }

    @Override
    public void preStart() {
        for (int i = 1; i <= config.getWarehouseCount(); i += config.getWarehousesPerDatabaseConnection()) {
            int range = Math.min(config.getWarehousesPerDatabaseConnection(), config.getWarehouseCount() - (i-1));
            getContext().actorOf(TerminalGroup.props(i, range, config, onFailure), "term-group-" + i);
        }
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new AllForOneStrategy(DeciderBuilder.matchAny(o -> {
            if (onFailure != null) {
                onFailure.accept(o);
            }
            // Surely there's a better way to do this...
            getContext().getSystem().terminate();
            return SupervisorStrategy.stop();
        }).build());
    }

    public static Props props(DatabaseConfig config) {
        return Props.create(RootActor.class, () -> new RootActor(config));
    }

    public static Props props(DatabaseConfig config, Consumer<Throwable> onFailure) {
        return Props.create(RootActor.class, () -> new RootActor(config, onFailure));
    }
}
