package net.jackw.olep.application;

import akka.actor.AbstractActor;
import akka.actor.Props;
import com.codahale.metrics.MetricRegistry;
import net.jackw.olep.common.Database;
import net.jackw.olep.edge.EventDatabase;

/**
 * A group of terminals that share a database connection, to optimise resource use
 */
public class TerminalGroup extends AbstractActor {
    private int startWarehouseId;
    private int warehouseIdRange;
    private MetricRegistry registry;
    private Database db;

    @SuppressWarnings("MustBeClosedChecker")
    public TerminalGroup(int startWarehouseId, int warehouseIdRange, MetricRegistry registry) {
        this.startWarehouseId = startWarehouseId;
        this.warehouseIdRange = warehouseIdRange;
        this.registry = registry;
        this.db = new EventDatabase("localhost:9092", "localhost");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals("close", _s -> getContext().stop(getSelf()))
            .build();
    }

    public static Props props(int startWarehouseId, int range, MetricRegistry registry) {
        return Props.create(TerminalGroup.class, () -> new TerminalGroup(startWarehouseId, range, registry));
    }

    @Override
    public void preStart() {
        for (int i = 0; i < warehouseIdRange; i++) {
            int warehouse = startWarehouseId + i;
            for (int district = 1; district <= 10; district++) {
                getContext().actorOf(Terminal.props(warehouse, district, db, registry), "term-" + warehouse + "-" + district);
            }
        }
    }

    @Override
    public void postStop() {
        db.close();
    }
}
