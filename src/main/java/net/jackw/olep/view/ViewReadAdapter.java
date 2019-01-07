package net.jackw.olep.view;

import net.jackw.olep.view.records.Customer;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ViewReadAdapter extends Remote {
    /**
     * Retrieve the customer details, and the details of their latest order
     *
     * @param customerId Customer to get details for
     * @param districtId Customer's district
     * @param warehouseId Customer's warehouse
     * @return The Customer view
     */
    Customer orderStatus(int customerId, int districtId, int warehouseId) throws RemoteException;

    /**
     * Retrieve the customer details, and the details of their latest order
     *
     * @param customerLastName Last name of the customer to get details for
     * @param districtId Customer's district
     * @param warehouseId Customer's warehouse
     * @return The Customer view
     */
    Customer orderStatus(String customerLastName, int districtId, int warehouseId) throws RemoteException;

    /**
     * Retrieve the stock-level transaction using the data from this view
     *
     * Stock-level loads the order lines associated with the 20 most recent orders to this district (regardless of the
     * supplying warehouse), and  stock level for the corresponding items in the home warehouse is loaded. The number of
     * items with a stock level < stockThreshold is returned.
     *
     * TODO: This should be some form of future
     *
     * @param districtId The district to look at orders from
     * @param warehouseId The warehouse to check the stock for
     * @param stockThreshold The lowest permitted stock level
     * @return The number of items from the checked orders that are < stockThreshold in this warehouse
     */
    int stockLevel(int districtId, int warehouseId, int stockThreshold) throws RemoteException;

    /*@Override
    void close();*/
}
