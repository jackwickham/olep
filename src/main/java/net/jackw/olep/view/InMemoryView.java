package net.jackw.olep.view;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.MapMaker;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.store.SharedCustomerStore;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.OrderLineModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import net.jackw.olep.common.records.OrderStatusResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryView extends UnicastRemoteObject implements ViewReadAdapter, ViewWriteAdapter {
    private Map<WarehouseSpecificKey, WarehouseItemStock> stockMap;
    private Map<WarehouseSpecificKey, Queue<Set<WarehouseItemStock>>> recentOrders;
    private Map<DistrictSpecificKey, CustomerState> customerState;
    private SharedCustomerStore customerSharedStore;

    private final DatabaseConfig config;

    private final Set<Integer> registeredPartitions;

    public InMemoryView(SharedCustomerStore customerSharedStore, DatabaseConfig config) throws RemoteException {
        super();
        stockMap = new MapMaker().initialCapacity(1000).weakValues().makeMap();
        recentOrders = new ConcurrentHashMap<>();
        customerState = new ConcurrentHashMap<>();
        this.customerSharedStore = customerSharedStore;
        registeredPartitions = Collections.synchronizedSet(new HashSet<>());

        this.config = config;
    }

    ///// Reads /////

    @Override
    public OrderStatusResult orderStatus(int customerId, int districtId, int warehouseId) {
        DistrictSpecificKey customerKey = new DistrictSpecificKey(customerId, districtId, warehouseId);
        CustomerShared customerShared = customerSharedStore.get(customerKey);
        return customerState.get(customerKey).intoOrderStatusResult(customerShared);
    }

    @Override
    public OrderStatusResult orderStatus(String customerLastName, int districtId, int warehouseId) {
        CustomerShared customerShared = customerSharedStore.get(new CustomerNameKey(customerLastName, districtId, warehouseId));
        return customerState.get(new DistrictSpecificKey(customerShared.id, districtId, warehouseId))
            .intoOrderStatusResult(customerShared);
    }

    @Override
    public int stockLevel(int districtId, int warehouseId, int stockThreshold) {
        log.debug("Processing stock level for {}.{} - threshold {}", districtId, warehouseId, stockThreshold);
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
        // Also update the customer's latest order
        DistrictSpecificKey customerKey = new DistrictSpecificKey(modification.customerId, modification.districtId, modification.warehouseId);
        boolean replaced;
        do {
            CustomerState oldState = customerState.get(customerKey);
            CustomerState newState;
            if (oldState == null) {
                newState = new CustomerState(new BigDecimal("-10.00"), modification.orderId, modification.date, null, modification.lines);
                replaced = customerState.putIfAbsent(customerKey, newState) == null;
            } else {
                newState = oldState.withLatestOrder(modification.orderId, modification.date, null, modification.lines);
                replaced = customerState.replace(customerKey, oldState, newState);
            }
        } while (!replaced); // Retry if concurrency meant that we didn't make the change
    }

    @Override
    public void delivery(DeliveryModification modification) {
        DistrictSpecificKey key = new DistrictSpecificKey(modification.customerId, modification.districtId, modification.warehouseId);
        boolean replaced;
        do {
            CustomerState state = customerState.get(key);
            replaced = customerState.replace(
                key, state,
                state.withDelivery(modification.orderId, modification.deliveryDate, modification.carrierId, modification.orderTotal)
            );
        } while (!replaced);
    }

    @Override
    public void payment(PaymentModification modification) {
        DistrictSpecificKey key = new DistrictSpecificKey(modification.customerId, modification.districtId, modification.warehouseId);
        boolean replaced;
        do {
            CustomerState state = customerState.get(key);
            replaced = customerState.replace(key, state, state.withPayment(modification.amount));
        } while (!replaced);
    }

    @Override
    public void remoteStock(RemoteStockModification modification) {
        getStockObject(new WarehouseSpecificKey(modification.itemId, modification.warehouseId), modification.stockLevel);
    }

    @Override
    public synchronized boolean register(int partition) {
        try {
            Registry registry = LocateRegistry.getRegistry(config.getViewRegistryHost());
            registry.rebind("view/" + partition, this);
            registeredPartitions.add(partition);
            return true;
        } catch (RemoteException e) {
            log.error("Failed to bind view", e);
            return false;
        }
    }

    @Override
    public synchronized void unregister(int partition) {
        if (registeredPartitions.remove(partition)) {
            // Unbind in the registry first
            unbindPartition(partition);
        }
        // Update adapter stores
        for (WarehouseSpecificKey district : recentOrders.keySet()) {
            if (district.warehouseId % config.getModificationTopicPartitions() == partition) {
                // No longer belongs here
                recentOrders.remove(district);
            }
        }
        for (WarehouseSpecificKey district : stockMap.keySet()) {
            if (district.warehouseId % config.getModificationTopicPartitions() == partition) {
                stockMap.remove(district);
            }
        }
        for (DistrictSpecificKey customer : customerState.keySet()) {
            if (customer.warehouseId % config.getModificationTopicPartitions() == partition) {
                customerState.remove(customer);
            }
        }
    }

    @Override
    public synchronized void close() {
        for (int partition: registeredPartitions) {
            unbindPartition(partition);
        }
        registeredPartitions.clear();
    }

    ///// Utils /////

    @CanIgnoreReturnValue
    private WarehouseItemStock getStockObject(WarehouseSpecificKey key, int newStock) {
        WarehouseItemStock stock = stockMap.computeIfAbsent(key, k -> new WarehouseItemStock());
        stock.setStock(newStock);
        return stock;
    }

    /**
     * Unregister partition in registry
     */
    private void unbindPartition(int partition) {
        try {
            Registry registry = LocateRegistry.getRegistry(config.getViewRegistryHost());
            registry.unbind("view/" + partition);
        } catch (NotBoundException e) {
            log.warn("Received NotBoundException while trying to unregister partition " + partition);
        } catch (RemoteException e) {
            log.error("Failed to unbind partition " + partition, e);
        }
    }


    private static Logger log = LogManager.getLogger();
}
