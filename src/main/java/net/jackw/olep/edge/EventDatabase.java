package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.view.ViewReadAdapter;
import net.jackw.olep.common.records.OrderStatusResult;

import java.math.BigDecimal;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Date;
import java.util.List;

public class EventDatabase implements Database {
    private final DatabaseConfig config;
    private final DatabaseConnection eventConnection;
    private final Registry registry;

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public EventDatabase(DatabaseConfig config) {
        this.config = config;
        try {
            eventConnection = new DatabaseConnection(config.getBootstrapServers());
            registry = LocateRegistry.getRegistry(config.getViewRegistryHost());
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private ViewReadAdapter getViewForWarehouse(int warehouse) throws RemoteException {
        int partition = warehouse % config.getModificationTopicPartitions();
        try {
            return (ViewReadAdapter) registry.lookup("view/" + partition);
        } catch (NotBoundException e) {
            // :(
            throw new RuntimeException(e);
        }
    }


    /**
     * Send a New-Order transaction
     */
    @Override
    public TransactionStatus<NewOrderResult> newOrder(
        int customerId, int districtId, int warehouseId, List<NewOrderRequest.OrderLine> lines
    ) {
        long orderDate = new Date().getTime();
        NewOrderRequest msgBody = new NewOrderRequest(
            customerId, districtId, warehouseId, ImmutableList.copyOf(lines), orderDate
        );
        return eventConnection.send(msgBody, new NewOrderResult.Builder(customerId, districtId, warehouseId, orderDate, lines))
            .getTransactionStatus();
    }

    /**
     * Send a Payment transaction by customer ID
     */
    @Override
    public TransactionStatus<PaymentResult> payment(
        int customerId, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId,
        BigDecimal amount
    ) {
        PaymentRequest msgBody = new PaymentRequest(
            customerId, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount
        );
        return eventConnection.send(msgBody, new PaymentResult.Builder(
            districtId, warehouseId, customerDistrictId, customerWarehouseId
        )).getTransactionStatus();
    }

    /**
     * Send a Payment transaction by customer last name
     */
    @Override
    public TransactionStatus<PaymentResult> payment(
        String customerLastName, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId,
        BigDecimal amount
    ) {
        PaymentRequest msgBody = new PaymentRequest(
            customerLastName, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount
        );
        return eventConnection.send(msgBody, new PaymentResult.Builder(
            districtId, warehouseId, customerDistrictId, customerWarehouseId
        )).getTransactionStatus();
    }

    /**
     * Send a Delivery transaction
     */
    @Override
    public TransactionStatus<DeliveryResult> delivery(int warehouseId, int carrierId) {
        DeliveryRequest msgBody = new DeliveryRequest(warehouseId, carrierId, new Date().getTime());
        return eventConnection.send(msgBody, new DeliveryResult.Builder(warehouseId, carrierId)).getTransactionStatus();
    }

    @Override
    public int stockLevel(int districtId, int warehouseId, int stockThreshold) {
        try {
            return getViewForWarehouse(warehouseId).stockLevel(districtId, warehouseId, stockThreshold);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OrderStatusResult orderStatus(int customerId, int districtId, int warehouseId) {
        try {
            return getViewForWarehouse(warehouseId).orderStatus(customerId, districtId, warehouseId);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OrderStatusResult orderStatus(String customerLastName, int districtId, int warehouseId) {
        try {
            return getViewForWarehouse(warehouseId).orderStatus(customerLastName, districtId, warehouseId);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        eventConnection.close();
    }
}
