package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.view.ViewReadAdapter;
import net.jackw.olep.common.records.OrderStatusResult;

import java.io.Closeable;
import java.math.BigDecimal;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.Date;
import java.util.List;

public class Database implements Closeable {
    private final DatabaseConnection eventConnection;
    private final ViewReadAdapter viewAdapter;

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public Database(String eventBootstrapServers, String viewServer) {
        try {
            viewAdapter = loadViewAdapter(viewServer);
            eventConnection = new DatabaseConnection(eventBootstrapServers);
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private ViewReadAdapter loadViewAdapter(String server) throws RemoteException, NotBoundException {
        return (ViewReadAdapter) LocateRegistry.getRegistry(server).lookup("view/TODO_PARTITION_NUMBER");
    }


    /**
     * Send a New-Order transaction
     */
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
    public TransactionStatus<DeliveryResult> delivery(int warehouseId, int carrierId) {
        DeliveryRequest msgBody = new DeliveryRequest(warehouseId, carrierId, new Date().getTime());
        return eventConnection.send(msgBody, new DeliveryResult.Builder(warehouseId, carrierId)).getTransactionStatus();
    }

    public int stockLevel(int warehouseId, int districtId, int stockThreshold) {
        try {
            return viewAdapter.stockLevel(districtId, warehouseId, stockThreshold);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    public OrderStatusResult orderStatus(int customerId, int districtId, int warehouseId) {
        try {
            return viewAdapter.orderStatus(customerId, districtId, warehouseId);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    public OrderStatusResult orderStatus(String customerLastName, int districtId, int warehouseId) {
        try {
            return viewAdapter.orderStatus(customerLastName, districtId, warehouseId);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // Use try-with-resources to ensure that they are closed
        try (
            DatabaseConnection d = eventConnection;
            //ViewReadAdapter v = viewAdapter;
        ) { }
    }
}
