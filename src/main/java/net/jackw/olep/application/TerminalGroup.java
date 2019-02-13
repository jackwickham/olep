package net.jackw.olep.application;

import akka.actor.AbstractActor;
import akka.actor.Props;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.edge.EventDatabase;

/**
 * A group of terminals that share a database connection, to optimise resource use
 */
public class TerminalGroup extends AbstractActor {
    private int startWarehouseId;
    private int warehouseIdRange;
    private DatabaseConfig config;
    private Database db;

    @SuppressWarnings("MustBeClosedChecker")
    public TerminalGroup(int startWarehouseId, int warehouseIdRange, DatabaseConfig config) {
        this.startWarehouseId = startWarehouseId;
        this.warehouseIdRange = warehouseIdRange;
        this.config = config;
        this.db = new EventDatabase(config.getBootstrapServers(), config.getViewRegistryHost());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals("close", _s -> getContext().stop(getSelf()))
            .build();
    }

    public static Props props(int startWarehouseId, int range, DatabaseConfig config) {
        return Props.create(TerminalGroup.class, () -> new TerminalGroup(startWarehouseId, range, config));
    }

    @Override
    public void preStart() {
        for (int i = 0; i < warehouseIdRange; i++) {
            int warehouse = startWarehouseId + i;
            for (int district = 1; district <= config.getDistrictsPerWarehouse(); district++) {
                getContext().actorOf(Terminal.props(warehouse, district, db, config), "term-" + warehouse + "-" + district);
            }
        }
    }

    @Override
    public void postStop() {
        db.close();
    }
}
