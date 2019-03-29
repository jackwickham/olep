package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.OrderLineResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import net.jackw.olep.utils.populate.RandomCustomerFactory;
import net.jackw.olep.utils.populate.RandomDistrictFactory;
import net.jackw.olep.utils.populate.RandomWarehouseFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
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
        try (
            ResultSet wh = connection.loadWarehouse(warehouseId);
            ResultSet dist = connection.loadDistrict(new WarehouseSpecificKey(districtId, warehouseId));
            ResultSet cust = connection.loadCustomer(new DistrictSpecificKey(customerId, districtId, warehouseId));
        ) {
            connection.incrementDistrictNextOrderId(new WarehouseSpecificKey(districtId, warehouseId));

            boolean allLocal = lines.stream().allMatch(line -> line.supplyingWarehouseId == warehouseId);
            int orderId = dist.getInt("NEXT_O_ID");
            long entryDate = new Date().getTime();

            connection.insertOrder(
                orderId , districtId, warehouseId, customerId, entryDate,
                null, lines.size(), allLocal
            );

            ImmutableList.Builder<OrderLineResult> lineResultBuilder = ImmutableList.builderWithExpectedSize(lines.size());

            int lineNumber = 0;
            for (NewOrderRequest.OrderLine line : lines) {
                WarehouseSpecificKey stockKey = new WarehouseSpecificKey(line.itemId, line.supplyingWarehouseId);
                try (
                    ResultSet item = connection.loadItem(line.itemId);
                    ResultSet stock = connection.loadStock(stockKey)
                ) {
                    if (item.isAfterLast()) {
                        connection.rollback();
                        return new TransactionStatus<>(
                            0,
                            Futures.immediateFuture(null),
                            Futures.immediateFailedFuture(new TransactionRejectedException()),
                            Futures.immediateCancelledFuture()
                        );
                    }

                    int stockQuantity = stock.getInt("QUANTITY");
                    int newStockQuantity;
                    if (stockQuantity - line.quantity >= 10) {
                        newStockQuantity = stockQuantity - line.quantity;
                    } else {
                        newStockQuantity = stockQuantity - line.quantity + 91;
                    }
                    int newStockRemoteCount = stock.getInt("REMOTE_CNT");
                    if (line.supplyingWarehouseId != warehouseId) {
                        newStockRemoteCount++;
                    }
                    connection.updateStock(
                        stockKey, newStockQuantity, stock.getInt("YTD") + line.quantity,
                        stock.getInt("ORDER_CNT") + 1, newStockRemoteCount
                    );

                    BigDecimal lineAmount = item.getBigDecimal("PRICE").multiply(new BigDecimal(line.quantity));

                    connection.insertOrderLine(
                        orderId, districtId, warehouseId, ++lineNumber, line.itemId, line.supplyingWarehouseId, null,
                        line.quantity, lineAmount, stock.getString(String.format("DIST_%02d", districtId))
                    );

                    lineResultBuilder.add(new OrderLineResult(
                        line.supplyingWarehouseId,
                        line.itemId,
                        item.getString("NAME"),
                        line.quantity,
                        newStockQuantity,
                        item.getBigDecimal("PRICE"),
                        lineAmount
                    ));
                }
            }

            connection.commit();

            NewOrderResult result = new NewOrderResult(
                customerId, districtId, warehouseId, entryDate, orderId, cust.getString("LAST"),
                Credit.fromByteValue(cust.getByte("CREDIT")), cust.getBigDecimal("DISCOUNT"),
                wh.getBigDecimal("TAX"), dist.getBigDecimal("TAX"), lineResultBuilder.build()
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
        WarehouseShared wh = RandomWarehouseFactory.getInstance().makeWarehouseShared();
        DistrictShared dist = RandomDistrictFactory.instanceFor(wh).makeDistrictShared();
        Customer cust = RandomCustomerFactory.instanceFor(dist, arguments.getConfig().getCustomerNameRange()).makeCustomer();
        db.connection.insertWarehouse(wh, new BigDecimal("30000.00"));
        db.connection.insertDistrict(dist, new BigDecimal("30000.00"), 3001);
        db.connection.insertCustomer(cust, new BigDecimal("10.00"), 1, 0);
        db.connection.commit();

        db.close();
    }
}
