package net.jackw.olep.edge;

import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.PaymentResult;

import java.math.BigDecimal;
import java.util.List;

public interface Database extends AutoCloseable {
    TransactionStatus<NewOrderResult> newOrder(
        int customerId, int districtId, int warehouseId, List<NewOrderRequest.OrderLine> lines
    );

    TransactionStatus<PaymentResult> payment(
        int customerId, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId,
        BigDecimal amount
    );

    TransactionStatus<PaymentResult> payment(
        String customerLastName, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId,
        BigDecimal amount
    );

    TransactionStatus<DeliveryResult> delivery(int warehouseId, int carrierId);

    int stockLevel(int districtId, int warehouseId, int stockThreshold);

    OrderStatusResult orderStatus(int customerId, int districtId, int warehouseId);

    OrderStatusResult orderStatus(String customerLastName, int districtId, int warehouseId);

    @Override
    void close();
}
