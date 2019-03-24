package net.jackw.olep.application;

import akka.actor.AbstractActor;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import net.jackw.olep.edge.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.EventDatabase;

import java.util.function.Consumer;

/**
 * A group of terminals that share a database connection, to optimise resource use
 */
public class TerminalGroup extends AbstractActor {
    private int startWarehouseId;
    private int warehouseIdRange;
    private DatabaseConfig config;
    private Consumer<Throwable> onFailure;
    private Database db;

    @SuppressWarnings("MustBeClosedChecker")
    public TerminalGroup(int startWarehouseId, int warehouseIdRange, DatabaseConfig config, Consumer<Throwable> onFailure) {
        this.startWarehouseId = startWarehouseId;
        this.warehouseIdRange = warehouseIdRange;
        this.config = config;
        this.onFailure = onFailure;
        this.db = new EventDatabase(config);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals("close", _s -> getContext().stop(getSelf()))
            .build();
    }

    public static Props props(int startWarehouseId, int range, DatabaseConfig config, Consumer<Throwable> onFailure) {
        return Props.create(TerminalGroup.class, () -> new TerminalGroup(startWarehouseId, range, config, onFailure));
    }

    @Override
    public void preStart() {
        for (int i = 0; i < warehouseIdRange; i++) {
            int warehouse = startWarehouseId + i;
            for (int district = 1; district <= config.getDistrictsPerWarehouse(); district++) {
                for (int term = 0; term < config.getTerminalsPerDistrict(); term++) {
                    getContext().actorOf(
                        Terminal.props(warehouse, district, db, config),
                        "term-" + warehouse + "-" + district + "-" + term
                    );
                }
            }
        }
    }

    @Override
    public void postStop() {
        db.close();
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new OneForOneStrategy(DeciderBuilder
            .match(TransactionTimeoutException.class, e -> {
                if (onFailure != null) {
                    onFailure.accept(e);
                }
                return SupervisorStrategy.restart();
            })
            .matchAny(t -> SupervisorStrategy.escalate())
            .build()
        );
    }
}
