package net.jackw.olep.view;

import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.PaymentModification;
import net.jackw.olep.view.records.Customer;

/**
 * Base implementation of a view adapter for key-value stores
 *
 * Stores must be provided for
 * <ul>
 *     <li>customerName, mapping customer district/warehouse/lastName to id</li>
 *     <li>customerDetails, mapping customer district/warehouse/id to Customer</li>
 *     <li>customerOrders, mapping customer district/warehouse/id to a sorted list of orders</li>
 *     <li>recentStock, mapping district/warehouse to a list of stock levels for recent items</li>
 * </ul>
 */
public abstract class KeyValueStoreAdapter implements ViewReadAdapter, ViewWriteAdapter {
    @Override
    public void newOrder(NewOrderModification modification) {
        Customer existingCustomer = getCustomerDetails(modification.customerId, modification.districtId, modification.warehouseId);
        Customer newCustomer = existingCustomer.withOrder(modification.orderId, modification.date, null, modification.lines);
        setCustomerDetails(modification.customerId, modification.districtId, modification.warehouseId, newCustomer);
    }

    @Override
    public void delivery(DeliveryModification modification) {

    }

    @Override
    public void payment(PaymentModification modification) {

    }

    @Override
    public int stockLevel(int warehouseId, int districtId, int stockThreshold) {
        // TODO
        return 0;
    }

    @Override
    public Customer orderStatus(int customerId, int districtId, int warehouseId) {
        return getCustomerDetails(customerId, districtId, warehouseId);
    }

    @Override
    public Customer orderStatus(String customerLastName, int districtId, int warehouseId) {
        return null;
    }

    protected abstract Customer getCustomerDetails(int customerId, int districtId, int warehouseId);

    protected abstract void setCustomerDetails(int customerId, int districtId, int warehouseId, Customer customer);
}
