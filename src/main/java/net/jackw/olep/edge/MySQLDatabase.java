package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.District;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.Order;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.Warehouse;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.OrderLineResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public class MySQLDatabase implements Database {
    private final MySQLConnection connection;

    public MySQLDatabase(DatabaseConfig config) {
        try {
            this.connection = new MySQLConnection(config);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TransactionStatus<NewOrderResult> newOrder(int customerId, int districtId, int warehouseId, List<NewOrderRequest.OrderLine> lines) {
        try {
            Warehouse wh = connection.loadWarehouse(warehouseId);
            District dist = connection.loadDistrict(new WarehouseSpecificKey(districtId, warehouseId));
            Customer cust = connection.loadCustomer(new DistrictSpecificKey(customerId, districtId, warehouseId));
            connection.incrementDistrictNextOrderId(new WarehouseSpecificKey(districtId, warehouseId));
            ImmutableList.Builder<OrderLine> lineBuilder = ImmutableList.builderWithExpectedSize(lines.size());
            ImmutableList.Builder<OrderLineResult> lineResultBuilder = ImmutableList.builderWithExpectedSize(lines.size());
            int lineNumber = 0;
            boolean allLocal = true;
            for (NewOrderRequest.OrderLine line : lines) {
                Item item = connection.loadItem(line.itemId);
                if (item == null) {
                    connection.rollback();
                    return new TransactionStatus<>(0, Futures.immediateFuture(null), Futures.immediateFailedFuture(new TransactionRejectedException()), Futures.immediateCancelledFuture());
                }
                Stock stock = connection.loadStock(new WarehouseSpecificKey(line.itemId, line.supplyingWarehouseId));
                BigDecimal lineAmount = item.price.multiply(new BigDecimal(line.quantity));
                lineBuilder.add(new OrderLine(
                    lineNumber++, line.itemId, line.supplyingWarehouseId, line.quantity, lineAmount,
                    stock.stockShared.getDistrictInfo(districtId)
                ));
                if (line.supplyingWarehouseId != warehouseId) {
                    allLocal = false;
                }
                lineResultBuilder.add(new OrderLineResult(
                    line.supplyingWarehouseId, line.itemId, item.name, line.quantity, stock.stockQuantity, item.price,
                    lineAmount
                ));
            }
            Order order = new Order(
                dist.nextOrderId, districtId, warehouseId, customerId, new Date().getTime(), null,
                lineBuilder.build(), allLocal
            );
            connection.insertOrder(order);
            connection.commit();

            NewOrderResult result = new NewOrderResult(
                customerId, districtId, warehouseId, order.entryDate, order.orderId, cust.customerShared.lastName,
                cust.customerShared.credit, cust.customerShared.discount, wh.tax, dist.tax, lineResultBuilder.build()
            );
            return resultToTransactionStatus(result);
        } catch (SQLException e) {
            RuntimeException r = new RuntimeException(e);
            try {
                connection.rollback();
            } catch (SQLException e1) {
                r.addSuppressed(e1);
            }
            throw r;
        }
    }

    @Override
    public TransactionStatus<PaymentResult> payment(int customerId, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId, BigDecimal amount) {
        return null;
    }

    @Override
    public TransactionStatus<PaymentResult> payment(String customerLastName, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId, BigDecimal amount) {
        return null;
    }

    @Override
    public TransactionStatus<DeliveryResult> delivery(int warehouseId, int carrierId) {
        return null;
    }

    @Override
    public int stockLevel(int districtId, int warehouseId, int stockThreshold) {
        return 0;
    }

    @Override
    public OrderStatusResult orderStatus(int customerId, int districtId, int warehouseId) {
        return null;
    }

    @Override
    public OrderStatusResult orderStatus(String customerLastName, int districtId, int warehouseId) {
        return null;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private <T extends TransactionResultMessage> TransactionStatus<T> resultToTransactionStatus(T result) {
        return new TransactionStatus<>(
            0,
            Futures.immediateFuture(null),
            Futures.immediateFuture(null),
            Futures.immediateFuture(result)
        );
    }

    public static void main(String[] args) throws IOException, SQLException {
        Arguments arguments = new Arguments(args);
        MySQLDatabase db = new MySQLDatabase(arguments.getConfig());
        db.connection.createTables();
        //db.newOrder(1, )
    }
}
