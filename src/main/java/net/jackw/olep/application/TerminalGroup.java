package net.jackw.olep.application;

import akka.actor.AbstractActor;
import akka.actor.Props;
import net.jackw.olep.edge.Database;

/**
 * A group of terminals that share a database connection, to optimise resource use
 */
public class TerminalGroup extends AbstractActor {
    private int startWarehouseId;
    private int warehouseIdRange;
    private Database db;

    @SuppressWarnings("MustBeClosedChecker")
    public TerminalGroup(int startWarehouseId, int warehouseIdRange) {
        this.startWarehouseId = startWarehouseId;
        this.warehouseIdRange = warehouseIdRange;
        this.db = new Database("localhost:9092", "localhost");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals("close", _s -> getContext().stop(getSelf()))
            .build();
    }

    public static Props props(int startWarehouseId, int range) {
        return Props.create(TerminalGroup.class, () -> new TerminalGroup(startWarehouseId, range));
    }

    @Override
    public void preStart() {
        for (int i = 0; i < warehouseIdRange; i++) {
            int warehouse = startWarehouseId + i;
            for (int district = 1; district <= 10; district++) {
                getContext().actorOf(Terminal.props(warehouse, district, db), "term-" + warehouse + "-" + district);
            }
        }
    }

    @Override
    public void postStop() {
        db.close();
    }
}
