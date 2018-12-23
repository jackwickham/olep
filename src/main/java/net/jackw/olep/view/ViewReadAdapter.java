package net.jackw.olep.view;

import net.jackw.olep.view.records.Customer;

import java.io.Closeable;

public interface ViewReadAdapter extends Closeable {
    /**
     * Retrieve the customer details, and the details of their latest order
     *
     * @param customerId Customer to get details for
     * @param districtId Customer's district
     * @param warehouseId Customer's warehouse
     * @return The Customer view
     */
    Customer orderStatus(int customerId, int districtId, int warehouseId);

    /**
     * Retrieve the customer details, and the details of their latest order
     *
     * @param customerLastName Last name of the customer to get details for
     * @param districtId Customer's district
     * @param warehouseId Customer's warehouse
     * @return The Customer view
     */
    Customer orderStatus(String customerLastName, int districtId, int warehouseId);

    /**
     * Retrieve the stock-level transaction using the data from this view
     *
     * Stock-level loads the order lines associated with the 20 most recent orders to this district (regardless of the
     * supplying warehouse), and  stock level for the corresponding items in the home warehouse is loaded. The number of
     * items with a stock level < stockThreshold is returned.
     *
     * TODO: This should be some form of future
     *
     * @param warehouseId The warehouse to check the stock for
     * @param districtId The district to look at orders from
     * @param stockThreshold The lowest permitted stock level
     * @return The number of items from the checked orders that are < stockThreshold in this warehouse
     */
    int stockLevel(int warehouseId, int districtId, int stockThreshold) ;
}
