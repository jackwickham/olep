package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.CustomerNameKey;
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
import net.jackw.olep.utils.populate.RandomItemFactory;
import net.jackw.olep.utils.populate.RandomStockFactory;
import net.jackw.olep.utils.populate.RandomWarehouseFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientException;
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
        return new Transaction<NewOrderResult>() {
            @Override
            public NewOrderResult exec() throws SQLException, TransactionRejectedException {
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
                        orderId, districtId, warehouseId, customerId, entryDate, null, lines.size(), allLocal
                    );
                    connection.insertNewOrder(orderId, districtId, warehouseId);

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
                                throw new TransactionRejectedException();
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
                    return result;
                }
            }
        }.run();
    }

    @Override
    public TransactionStatus<PaymentResult> payment(int customerId, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId, BigDecimal amount) {
        return new Transaction<PaymentResult>() {
            @Override
            public PaymentResult exec() throws SQLException {
                try (ResultSet cust = connection.loadCustomer(new DistrictSpecificKey(customerId, customerDistrictId, customerWarehouseId))) {
                    return performPayment(districtId, warehouseId, amount, cust);
                }
            }
        }.run();
    }

    @Override
    public TransactionStatus<PaymentResult> payment(String customerLastName, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId, BigDecimal amount) {
        return new Transaction<PaymentResult>() {
            @Override
            public PaymentResult exec() throws SQLException {
                try (ResultSet resultSet = connection.loadCustomersByName(new CustomerNameKey(customerLastName, customerDistrictId, customerWarehouseId))) {
                    resultSet.last();
                    int numResults = resultSet.getRow();
                    resultSet.absolute((numResults + 1) / 2);
                    return performPayment(districtId, warehouseId, amount, resultSet);
                }
            }
        }.run();
    }

    private PaymentResult performPayment(int districtId, int warehouseId, BigDecimal amount, ResultSet customer) throws SQLException {
        BigDecimal customerBalance = customer.getBigDecimal("BALANCE").subtract(amount);
        BigDecimal customerYtdPayment = customer.getBigDecimal("YTD_PAYMENT").add(amount);
        int customerPaymentCount = customer.getInt("PAYMENT_CNT") + 1;
        int customerId = customer.getInt("ID");
        int customerDistrictId = customer.getInt("D_ID");
        int customerWarehouseId = customer.getInt("W_ID");
        String customerData = customer.getString("DATA");
        Credit customerCredit = Credit.fromByteValue(customer.getByte("CREDIT"));
        if (customerCredit == Credit.BC) {
            StringBuilder builder = new StringBuilder(Math.min(customerData.length() + 20, 500));
            builder.append(customerId)
                .append(customerDistrictId)
                .append(customerWarehouseId)
                .append(districtId)
                .append(warehouseId)
                .append(amount);
            builder.append(customerData, 0, Math.min(customerData.length(), 500 - builder.length()));
            customerData = builder.toString();
        }

        connection.paymentUpdateCustomer(
            customerId, customerDistrictId, customerWarehouseId, customerBalance, customerYtdPayment,
            customerPaymentCount, customerData
        );

        try (
            ResultSet wh = connection.loadWarehouse(warehouseId);
            ResultSet dist = connection.loadDistrict(new WarehouseSpecificKey(districtId, warehouseId));
            ) {
            connection.setWarehouseYtd(warehouseId, wh.getInt("YTD") + 1);
            connection.setDistrictYtd(districtId, warehouseId, dist.getInt("YTD") + 1);

            String historyData = wh.getString("NAME") + "    " + dist.getString("NAME");
            connection.insertHistory(
                customerDistrictId, customerWarehouseId, districtId, warehouseId, new Date().getTime(), amount, historyData
            );

            connection.commit();

            return new PaymentResult(
                districtId, connection.getAddress(dist), warehouseId, connection.getAddress(wh), customerId, customerDistrictId,
                customerWarehouseId, customer.getString("FIRST"), customer.getString("MIDDLE"),
                customer.getString("LAST"), connection.getAddress(customer), customer.getString("PHONE"),
                customer.getLong("SINCE"), customerCredit, customer.getBigDecimal("CREDIT_LIM"),
                customer.getBigDecimal("DISCOUNT"), customerBalance, customerData
            );
        }
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

    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        Arguments arguments = new Arguments(args);
        MySQLDatabase db = new MySQLDatabase(arguments.getConfig());
        db.connection.createTables();
        WarehouseShared wh = RandomWarehouseFactory.getInstance().makeWarehouseShared();
        DistrictShared dist = RandomDistrictFactory.instanceFor(wh).makeDistrictShared();
        Customer cust = RandomCustomerFactory.instanceFor(dist, arguments.getConfig().getCustomerNameRange()).makeCustomer();
        db.connection.insertWarehouse(wh, new BigDecimal("30000.00"));
        db.connection.insertDistrict(dist, new BigDecimal("30000.00"), 3001);
        db.connection.insertCustomer(cust, new BigDecimal("10.00"), 1, 0);
        db.connection.insertItem(RandomItemFactory.getInstance().makeItem());
        db.connection.insertStock(RandomStockFactory.instanceFor(wh).makeStock(), 0, 0, 0);
        db.connection.commit();

        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                try (MySQLDatabase db2 = new MySQLDatabase(arguments.getConfig())) {
                    db2.newOrder(1, 1, 1, ImmutableList.of(new NewOrderRequest.OrderLine(1, 1, 2)));
                }
            }).start();
        }

        Thread.sleep(1000);

        db.payment(1, 1, 1, 1, 1, new BigDecimal("10.12"));
        db.payment("BARBARBAR", 1, 1, 1, 1, new BigDecimal("10.12"));

        db.close();
    }

    private abstract class Transaction<T extends TransactionResultMessage> {
        public abstract T exec() throws SQLException, TransactionRejectedException;

        public TransactionStatus<T> run() {
            do {
                try {
                    T result = exec();
                    return new TransactionStatus<>(
                        0,
                        Futures.immediateFuture(null),
                        Futures.immediateFuture(null),
                        Futures.immediateFuture(result)
                    );
                } catch (SQLTransientException e) {
                    // This will occur if there's an error during commit
                    // retry
                    System.out.println("Retrying");
                } catch (SQLException e) {
                    System.out.println(e);
                    RuntimeException re = new RuntimeException(e);
                    try {
                        connection.rollback();
                    } catch (SQLException e2) {
                        re.addSuppressed(e2);
                    }
                    throw re;
                } catch (TransactionRejectedException e) {
                    System.out.println("Rejected");
                    return new TransactionStatus<>(
                        0,
                        Futures.immediateFuture(null),
                        Futures.immediateFailedFuture(e),
                        SettableFuture.create()
                    );
                }
            } while (true);
        }
    }
}
