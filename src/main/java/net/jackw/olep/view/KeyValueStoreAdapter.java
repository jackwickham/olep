package net.jackw.olep.view;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.OrderLineModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import net.jackw.olep.view.records.Customer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base implementation of a view adapter for key-value stores
 *
 * The stores that need to be implemented are:
 * <ul>
 *     <li>customerName, mapping customer district/warehouse/lastName to id</li>
 *     <li>customerDetails, mapping customer district/warehouse/id to Customer</li>
 *     <li>stockLevel, mapping warehouse/item to stock level</li>
 *     <li>recentItems, mapping warehouse/district to the items in the 20 most recent orders to that district</li>
 * </ul>
 */
public abstract class KeyValueStoreAdapter implements ViewReadAdapter, ViewWriteAdapter {
    // Writes

    @Override
    public void newOrder(NewOrderModification modification) {
        // Update the customer with their new latest order
        Customer existingCustomer = getCustomerDetails(modification.customerId, modification.districtId, modification.warehouseId);
        Customer newCustomer = existingCustomer.withOrder(
            modification.orderId, modification.date, null,
            ImmutableList.copyOf(Lists.transform(modification.lines, OrderLineModification::toOrderLine))
        );
        setCustomerDetails(modification.customerId, modification.districtId, modification.warehouseId, newCustomer);

        // Update the stock level for the home warehouse
        Map<Integer, Integer> stockLevels = new HashMap<>(modification.lines.size());
        for (OrderLineModification line : modification.lines) {
            stockLevels.put(line.itemId, line.homeWarehouseStockLevel);
        }
        updateStockLevels(modification.warehouseId, stockLevels);
        addOrderItems(modification.districtId, modification.warehouseId, stockLevels.keySet());
    }

    @Override
    public void delivery(DeliveryModification modification) {
        // Update the customer with the new balance, and mark their order as delivered if it is their latest order
        Customer existingCustomer = getCustomerDetails(modification.customerId, modification.districtId, modification.warehouseId);
        Customer newCustomer = existingCustomer.afterDelivery(
            modification.orderId, modification.deliveryDate, modification.carrierId, modification.orderTotal
        );
        setCustomerDetails(modification.customerId, modification.districtId, modification.warehouseId, newCustomer);
    }

    @Override
    public void payment(PaymentModification modification) {
        // Update the customer's balance
        Customer existingCustomer = getCustomerDetails(modification.customerId, modification.districtId, modification.warehouseId);
        Customer newCustomer = existingCustomer.withBalance(modification.balance);
        setCustomerDetails(modification.customerId, modification.districtId, modification.warehouseId, newCustomer);
    }

    @Override
    public void addCustomer(Customer cust) {
        setCustomerDetails(cust.id, cust.districtId, cust.warehouseId, cust);
        // TODO: Name mapping
    }

    @Override
    public void remoteStock(RemoteStockModification modification) {
        // TODO
    }

    // Reads

    @Override
    public int stockLevel(int districtId, int warehouseId, int stockThreshold) {
        Collection<Integer> recentStockLevels = getRecentStockLevels(districtId, warehouseId);
        int belowThreshold = 0;
        for (int level : recentStockLevels) {
            if (level < stockThreshold ) {
                ++belowThreshold;
            }
        }
        return belowThreshold;
    }

    protected Collection<Integer> getRecentStockLevels(int districtId, int warehouseId) {
        Collection<Integer> items = getRecentItems(districtId, warehouseId);
        if (items.isEmpty()) {
            return items;
        }
        return getStockLevels(items, warehouseId);
    }

    @Override
    public Customer orderStatus(int customerId, int districtId, int warehouseId) {
        return getCustomerDetails(customerId, districtId, warehouseId);
    }

    @Override
    public Customer orderStatus(String customerLastName, int districtId, int warehouseId) {
        // TODO: This isn't being populated yet
        int customerId = getCustomerByName(customerLastName, districtId, warehouseId);
        return getCustomerDetails(customerId, districtId, warehouseId);
    }

    // Methods for override

    /**
     * Get the Customer object associated with the requested customer
     */
    protected abstract Customer getCustomerDetails(int customerId, int districtId, int warehouseId);

    /**
     * Update the customer object associated with the requested customer
     */
    protected abstract void setCustomerDetails(int customerId, int districtId, int warehouseId, Customer customer);

    /**
     * Get the customer ID associated with the provided name and district
     */
    protected abstract int getCustomerByName(String lastName, int districtId, int warehouseId);

    /**
     * Update the customer that should be associated with the name
     */
    protected abstract void setCustomerNameMapping(String lastName, int districtId, int warehouseId, int customerId);

    protected abstract Collection<Integer> getRecentItems(int districtId, int warehouseId);

    protected abstract void addOrderItems(int districtId, int warehouseId, Collection<Integer> items);

    protected abstract Collection<Integer> getStockLevels(Collection<Integer> items, int warehouseId);

    protected abstract void updateStockLevels(int warehouseId, Map<Integer, Integer> stockLevels);
}
