package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.message.transaction_request.DeliveryRequest;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.view.RedisAdapter;
import net.jackw.olep.view.ViewReadAdapter;
import net.jackw.olep.view.records.Customer;

import java.io.Closeable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

public class Database implements Closeable {
    private final DatabaseConnection eventConnection;
    private final ViewReadAdapter viewAdapter;

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public Database(String eventBootstrapServers, String viewServer) {
        try {
            viewAdapter = new RedisAdapter(viewServer);
            eventConnection = new DatabaseConnection(eventBootstrapServers);
        } catch (Exception e) {
            close();
            throw e;
        }
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
        return viewAdapter.stockLevel(warehouseId, districtId, stockThreshold);
    }

    public Customer orderStatus(int customerId, int districtId, int warehouseId) {
        return viewAdapter.orderStatus(customerId, districtId, warehouseId);
    }

    public Customer orderStatus(String customerLastName, int districtId, int warehouseId) {
        return viewAdapter.orderStatus(customerLastName, districtId, warehouseId);
    }

    @Override
    public void close() {
        // Use try-with-resources to ensure that they are closed
        try (
            DatabaseConnection d = eventConnection;
            ViewReadAdapter v = viewAdapter;
        ) { }
    }
}
