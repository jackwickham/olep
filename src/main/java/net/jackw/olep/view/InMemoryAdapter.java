package net.jackw.olep.view;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.MapMaker;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.OrderLineModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import net.jackw.olep.view.records.Customer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class InMemoryAdapter extends UnicastRemoteObject implements ViewReadAdapter, ViewWriteAdapter {
    private Map<WarehouseSpecificKey, WarehouseItemStock> stockMap;
    private Map<WarehouseSpecificKey, Queue<Set<WarehouseItemStock>>> recentOrders;

    public InMemoryAdapter() throws RemoteException {
        super();
        stockMap = new MapMaker().initialCapacity(1000).weakValues().makeMap();
        recentOrders = new HashMap<>();
    }

    ///// Reads /////

    @Override
    public Customer orderStatus(int customerId, int districtId, int warehouseId) {
        return null;
    }

    @Override
    public Customer orderStatus(String customerLastName, int districtId, int warehouseId) {
        return null;
    }

    @Override
    public int stockLevel(int districtId, int warehouseId, int stockThreshold) {
        log.debug("Processing stock level for {}.{} - threshold {}", districtId, warehouseId, stockThreshold);
        log.debug(stockMap);
        Queue<Set<WarehouseItemStock>> queue = recentOrders.get(
            new WarehouseSpecificKey(districtId, warehouseId)
        );
        if (queue == null) {
            return 0;
        }

        int belowThreshold = 0;
        Set<WarehouseItemStock> stocks = new HashSet<>(200);
        synchronized (queue) {
            for (Set<WarehouseItemStock> orderItems : queue) {
                for (WarehouseItemStock item : orderItems) {
                    if (!stocks.contains(item)) {
                        stocks.add(item);
                        if (item.getStock() < stockThreshold) {
                            ++belowThreshold;
                        }
                    }
                    // Otherwise we already sorted this item
                }
            }
        }
        return belowThreshold;
    }

    @Override
    public void close() {

    }

    ///// Writes //////

    @Override
    public void newOrder(NewOrderModification modification) {
        // Create a mapping of items in this order to the updated stock level
        // NB: This violates isolation, but the chances of it being observed are very slim, and fixing it would be
        // very expensive
        Set<WarehouseItemStock> itemStocks = new HashSet<>(modification.lines.size());
        for (OrderLineModification line : modification.lines) {
            itemStocks.add(getStockObject(
                new WarehouseSpecificKey(line.itemId, modification.warehouseId),
                line.homeWarehouseStockLevel
            ));
        }
        // Add the order to this district's queue of recent orders, creating the queue if needed
        Queue<Set<WarehouseItemStock>> queue = recentOrders.computeIfAbsent(
            new WarehouseSpecificKey(modification.districtId, modification.warehouseId),
            k -> EvictingQueue.create(20)
        );
        synchronized (queue) {
            queue.add(itemStocks);
        }
        log.debug("new order insert - {}", queue);
    }

    @Override
    public void delivery(DeliveryModification modification) {

    }

    @Override
    public void payment(PaymentModification modification) {

    }

    @Override
    public void remoteStock(RemoteStockModification modification) {
        getStockObject(new WarehouseSpecificKey(modification.itemId, modification.warehouseId), modification.stockLevel);
    }

    @Override
    public void addCustomer(Customer cust) {

    }

    ///// Utils /////

    @CanIgnoreReturnValue
    private WarehouseItemStock getStockObject(WarehouseSpecificKey key, int newStock) {
        WarehouseItemStock stock = stockMap.computeIfAbsent(key, k -> new WarehouseItemStock());
        stock.setStock(newStock);
        return stock;
    }


    private static Logger log = LogManager.getLogger();
}
