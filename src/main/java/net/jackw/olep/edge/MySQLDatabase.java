package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ForOverride;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.CloseablePool;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.DeliveryResult;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.OrderLineResult;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MySQLDatabase implements Database {
    private final CloseablePool<MySQLConnection> connectionPool;
    private final ExecutorService executorService;

    public MySQLDatabase(DatabaseConfig config) {
        int poolSize = config.getWarehousesPerDatabaseConnection() * config.getDistrictsPerWarehouse() * config.getTerminalsPerDistrict() / 70;
        this.connectionPool = new CloseablePool<>(() -> {
            try {
                return new MySQLConnection(config);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, poolSize);
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public TransactionStatus<NewOrderResult> newOrder(int customerId, int districtId, int warehouseId, List<NewOrderRequest.OrderLine> lines) {
        return execute(new NewOrderTransaction(customerId, districtId, warehouseId, lines));
    }

    @Override
    public TransactionStatus<PaymentResult> payment(int customerId, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId, BigDecimal amount) {
        return execute(new PaymentTransaction(districtId, warehouseId, amount, customerId, customerDistrictId, customerWarehouseId));
    }

    @Override
    public TransactionStatus<PaymentResult> payment(String customerLastName, int districtId, int warehouseId, int customerDistrictId, int customerWarehouseId, BigDecimal amount) {
        return execute(new PaymentTransaction(districtId, warehouseId, amount, customerLastName, customerDistrictId, customerWarehouseId));
    }

    @Override
    public TransactionStatus<DeliveryResult> delivery(int warehouseId, int carrierId) {
        final SettableFuture<Void> writtenToLogFuture = SettableFuture.create();
        final SettableFuture<Void> acceptedFuture = SettableFuture.create();
        final SettableFuture<DeliveryResult> completeFuture = SettableFuture.create();

        executorService.execute(() -> {
            ImmutableMap.Builder<Integer, Integer> processedOrders = ImmutableMap.builderWithExpectedSize(10);
            try {
                for (int districtId = 1; districtId <= 10; districtId++) {
                    Integer orderId = new DeliveryTransaction(warehouseId, districtId, carrierId).run();
                    if (orderId != null) {
                        processedOrders.put(districtId, orderId);
                    }
                }
                writtenToLogFuture.set(null);
                acceptedFuture.set(null);
                completeFuture.set(new DeliveryResult(warehouseId, carrierId, processedOrders.build()));
            } catch (TransactionRejectedException e) {
                throw new AssertionError("Delivery transaction should never be rejected", e);
            } catch (InterruptedException e) {
                writtenToLogFuture.setException(e);
            }
        });

        return new TransactionStatus<>(0, writtenToLogFuture, acceptedFuture, completeFuture);
    }

    @Override
    public int stockLevel(int districtId, int warehouseId, int stockThreshold) {
        try {
            return new StockLevelTransaction(districtId, warehouseId, stockThreshold).run();
        } catch (TransactionRejectedException e) {
            throw new AssertionError("Stock level should never be rejected", e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OrderStatusResult orderStatus(int customerId, int districtId, int warehouseId) {
        try {
            return new OrderStatusTransaction(customerId, districtId, warehouseId).run();
        } catch (TransactionRejectedException e) {
            throw new AssertionError("Order Status should never be rejected", e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OrderStatusResult orderStatus(String customerLastName, int districtId, int warehouseId) {
        try {
            return new OrderStatusTransaction(customerLastName, districtId, warehouseId).run();
        } catch (TransactionRejectedException e) {
            throw new AssertionError("Order Status should never be rejected", e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
        boolean interrupted = false;
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
        }
        try {
            connectionPool.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private <T extends TransactionResultMessage> TransactionStatus<T> execute(Transaction<T> transaction) {
        final SettableFuture<Void> writtenToLogFuture = SettableFuture.create();
        final SettableFuture<Void> acceptedFuture = SettableFuture.create();
        final SettableFuture<T> completeFuture = SettableFuture.create();
        executorService.execute(() -> {
            try {
                T result = transaction.run();
                writtenToLogFuture.set(null);
                acceptedFuture.set(null);
                completeFuture.set(result);
            } catch (TransactionRejectedException e) {
                writtenToLogFuture.set(null);
                acceptedFuture.setException(e);
            } catch (InterruptedException e) {
                writtenToLogFuture.setException(e);
            }
        });

        return new TransactionStatus<>(0, writtenToLogFuture, acceptedFuture, completeFuture);
    }

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    private ResultSet loadCustomerByName(MySQLConnection connection, String lastName, int districtId, int warehouseId) throws SQLException {
        ResultSet resultSet = connection.loadCustomersByName(new CustomerNameKey(lastName, districtId, warehouseId));
        try {
            resultSet.last();
            int numResults = resultSet.getRow();
            resultSet.absolute((numResults + 1) / 2);
        } catch (Throwable e) {
            resultSet.close();
            throw e;
        }
        return resultSet;
    }

    private abstract class Transaction<T> {
        @ForOverride
        protected abstract T exec(MySQLConnection connection) throws SQLException, TransactionRejectedException;

        public T run() throws TransactionRejectedException, InterruptedException {
            do {
                try (CloseablePool<MySQLConnection>.Resource connectionResource = connectionPool.acquire()) {
                    try {
                        T result = exec(connectionResource.get());
                        connectionResource.get().commit();
                        return result;
                    } catch (SQLTransientException e) {
                        // This will occur if there's an error during commit
                        // retry
                    } catch (SQLException e) {
                        log.error(e);
                        RuntimeException re = new RuntimeException(e);
                        try {
                            connectionResource.get().rollback();
                        } catch (SQLException e2) {
                            re.addSuppressed(e2);
                        }
                        throw re;
                    } catch (TransactionRejectedException e) {
                        try {
                            connectionResource.get().rollback();
                        } catch (SQLException se) {
                            RuntimeException re = new RuntimeException(se);
                            re.addSuppressed(e);
                            throw re;
                        }
                        throw e;
                    }
                }
            } while (true);
        }
    }

    private class NewOrderTransaction extends Transaction<NewOrderResult> {
        private final int customerId;
        private final int districtId;
        private final int warehouseId;
        private final List<NewOrderRequest.OrderLine> lines;

        public NewOrderTransaction(int customerId, int districtId, int warehouseId, List<NewOrderRequest.OrderLine> lines) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.customerId = customerId;
            this.lines = lines;
        }

        @Override
        protected NewOrderResult exec(MySQLConnection connection) throws SQLException, TransactionRejectedException {
            try (
                ResultSet wh = connection.loadWarehouse(warehouseId);
                ResultSet dist = connection.loadDistrict(new WarehouseSpecificKey(districtId, warehouseId));
                ResultSet cust = connection.loadCustomer(new DistrictSpecificKey(customerId, districtId, warehouseId));
            ) {
                int orderId = dist.getInt("D_NEXT_O_ID");

                connection.setDistrictNextOrderId(new WarehouseSpecificKey(districtId, warehouseId), orderId + 1);

                boolean allLocal = lines.stream().allMatch(line -> line.supplyingWarehouseId == warehouseId);
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
                        if (!item.isFirst()) {
                            throw new TransactionRejectedException();
                        }

                        int stockQuantity = stock.getInt("S_QUANTITY");
                        int newStockQuantity;
                        if (stockQuantity - line.quantity >= 10) {
                            newStockQuantity = stockQuantity - line.quantity;
                        } else {
                            newStockQuantity = stockQuantity - line.quantity + 91;
                        }
                        int newStockRemoteCount = stock.getInt("S_REMOTE_CNT");
                        if (line.supplyingWarehouseId != warehouseId) {
                            newStockRemoteCount++;
                        }
                        connection.updateStock(
                            stockKey, newStockQuantity, stock.getInt("S_YTD") + line.quantity,
                            stock.getInt("S_ORDER_CNT") + 1, newStockRemoteCount
                        );

                        BigDecimal lineAmount = item.getBigDecimal("I_PRICE").multiply(new BigDecimal(line.quantity));

                        connection.insertOrderLine(
                            orderId, districtId, warehouseId, ++lineNumber, line.itemId, line.supplyingWarehouseId, null,
                            line.quantity, lineAmount, stock.getString(String.format("S_DIST_%02d", districtId))
                        );

                        lineResultBuilder.add(new OrderLineResult(
                            line.supplyingWarehouseId,
                            line.itemId,
                            item.getString("I_NAME"),
                            line.quantity,
                            newStockQuantity,
                            item.getBigDecimal("I_PRICE"),
                            lineAmount
                        ));
                    }
                }

                return new NewOrderResult(
                    customerId, districtId, warehouseId, entryDate, orderId, cust.getString("C_LAST"),
                    Credit.fromByteValue(cust.getByte("C_CREDIT")), cust.getBigDecimal("C_DISCOUNT"),
                    wh.getBigDecimal("W_TAX"), dist.getBigDecimal("D_TAX"), lineResultBuilder.build()
                );
            }
        }
    }

    private class PaymentTransaction extends Transaction<PaymentResult> {
        private final int districtId;
        private final int warehouseId;
        private final BigDecimal amount;

        private final String customerName;
        private final int customerId;
        private final int customerDistrictId;
        private final int customerWarehouseId;

        public PaymentTransaction(int districtId, int warehouseId, BigDecimal amount, int customerId, int customerDistrictId, int customerWarehouseId) {
            this.districtId = districtId;
            this.warehouseId = warehouseId;
            this.amount = amount;
            this.customerId = customerId;
            this.customerDistrictId = customerDistrictId;
            this.customerWarehouseId = customerWarehouseId;
            this.customerName = null;
        }

        public PaymentTransaction(int districtId, int warehouseId, BigDecimal amount, String customerName, int customerDistrictId, int customerWarehouseId) {
            this.districtId = districtId;
            this.warehouseId = warehouseId;
            this.amount = amount;
            this.customerDistrictId = customerDistrictId;
            this.customerWarehouseId = customerWarehouseId;
            this.customerName = customerName;
            this.customerId = -1;
        }

        @Override
        protected PaymentResult exec(MySQLConnection connection) throws SQLException {
            if (customerName == null) {
                try (ResultSet cust = connection.loadCustomer(new DistrictSpecificKey(customerId, customerDistrictId, customerWarehouseId))) {
                    return performPayment(connection, cust);
                }
            } else {
                try (ResultSet cust = loadCustomerByName(connection, customerName, customerDistrictId, customerWarehouseId)) {
                    return performPayment(connection, cust);
                }
            }
        }

        protected PaymentResult performPayment(MySQLConnection connection, ResultSet customer) throws SQLException {
            BigDecimal customerBalance = customer.getBigDecimal("C_BALANCE").subtract(amount);
            BigDecimal customerYtdPayment = customer.getBigDecimal("C_YTD_PAYMENT").add(amount);
            int customerPaymentCount = customer.getInt("C_PAYMENT_CNT") + 1;
            int customerId = customer.getInt("C_ID");
            int customerDistrictId = customer.getInt("C_D_ID");
            int customerWarehouseId = customer.getInt("C_W_ID");
            String customerData = customer.getString("C_DATA");
            Credit customerCredit = Credit.fromByteValue(customer.getByte("C_CREDIT"));
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
                connection.setWarehouseYtd(warehouseId, wh.getInt("W_YTD") + 1);
                connection.setDistrictYtd(districtId, warehouseId, dist.getInt("D_YTD") + 1);

                String historyData = wh.getString("W_NAME") + "    " + dist.getString("D_NAME");
                connection.insertHistory(
                    customerDistrictId, customerWarehouseId, districtId, warehouseId, new Date().getTime(), amount, historyData
                );

                return new PaymentResult(
                    districtId, connection.getAddress(dist, "D"), warehouseId, connection.getAddress(wh, "W"),
                    customerId, customerDistrictId, customerWarehouseId, customer.getString("C_FIRST"),
                    customer.getString("C_MIDDLE"), customer.getString("C_LAST"),
                    connection.getAddress(customer, "C"), customer.getString("C_PHONE"),
                    customer.getLong("C_SINCE"), customerCredit, customer.getBigDecimal("C_CREDIT_LIM"),
                    customer.getBigDecimal("C_DISCOUNT"), customerBalance, customerData
                );
            }
        }
    }

    private class DeliveryTransaction extends Transaction<Integer> {
        private final int warehouseId;
        private final int districtId;
        private final int carrierId;

        public DeliveryTransaction(int warehouseId, int districtId, int carrierId) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.carrierId = carrierId;
        }

        @Override
        protected Integer exec(MySQLConnection connection) throws SQLException {
            try (ResultSet results = connection.loadLatestNewOrder(districtId, warehouseId)) {
                if (results.next()) {
                    int orderId = results.getInt("NO_O_ID");
                    connection.deleteNewOrder(orderId, districtId, warehouseId);
                    connection.setOrderCarrierId(orderId, districtId, warehouseId, carrierId);
                    connection.setOrderLineDeliveryDate(orderId, districtId, warehouseId, new Date().getTime());
                    BigDecimal amount = connection.getOrderLineAmountTotal(orderId, districtId, warehouseId);
                    connection.deliveryUpdateCustomer(results.getInt("O_C_ID"), districtId, warehouseId, amount);

                    return orderId;
                } else {
                    return null;
                }
            }
        }
    }

    private class StockLevelTransaction extends Transaction<Integer> {
        private final int districtId;
        private final int warehouseId;
        private final int stockThreshold;

        public StockLevelTransaction(int districtId, int warehouseId, int stockThreshold) {
            this.districtId = districtId;
            this.warehouseId = warehouseId;
            this.stockThreshold = stockThreshold;
        }

        @Override
        protected Integer exec(MySQLConnection connection) throws SQLException {
            return connection.getStockLevel(districtId, warehouseId, stockThreshold);
        }
    }

    private class OrderStatusTransaction extends Transaction<OrderStatusResult> {
        private final String customerLastName;
        private final int customerId;

        private final int districtId;
        private final int warehouseId;

        public OrderStatusTransaction(int customerId, int districtId, int warehouseId) {
            this.customerId = customerId;
            this.districtId = districtId;
            this.warehouseId = warehouseId;
            this.customerLastName = null;
        }

        public OrderStatusTransaction(String customerLastName, int districtId, int warehouseId) {
            this.customerLastName = customerLastName;
            this.districtId = districtId;
            this.warehouseId = warehouseId;
            this.customerId = -1;
        }

        @MustBeClosed
        private ResultSet getCustomer(MySQLConnection connection) throws SQLException {
            if (customerLastName != null) {
                return loadCustomerByName(connection, customerLastName, districtId, warehouseId);
            } else {
                return connection.loadCustomer(new DistrictSpecificKey(customerId, districtId, warehouseId));
            }
        }

        @Override
        protected OrderStatusResult exec(MySQLConnection connection) throws SQLException {
            try (ResultSet customer = getCustomer(connection)) {
                int customerId = customer.getInt("C_ID");
                try (ResultSet orderLines = connection.getLatestOrder(customerId, districtId, warehouseId)) {
                    orderLines.next();

                    int orderId = orderLines.getInt("O_ID");
                    long orderDate = orderLines.getLong("O_ENTRY_D");
                    Integer carrierId = orderLines.getInt("O_CARRIER_ID");
                    if (orderLines.wasNull()) {
                        carrierId = null;
                    }

                    ImmutableList.Builder<OrderLine> lines = ImmutableList.builder();
                    do {
                        Long deliveryDate = null;
                        if (carrierId != null) {
                            deliveryDate = orderLines.getLong("OL_DELIVERY_D");
                        }
                        lines.add(new OrderLine(
                            orderLines.getInt("OL_NUMBER"), orderLines.getInt("OL_I_ID"),
                            orderLines.getInt("OL_SUPPLY_W_ID"), deliveryDate, orderLines.getInt("OL_QUANTITY"),
                            orderLines.getBigDecimal("OL_AMOUNT"), orderLines.getString("OL_DIST_INFO")
                        ));
                    } while (orderLines.next());

                    return new OrderStatusResult(
                        customerId, districtId, warehouseId, customer.getString("C_FIRST"),
                        customer.getString("C_MIDDLE"), customer.getString("C_LAST"),
                        customer.getBigDecimal("C_BALANCE"), orderId, orderDate, carrierId, lines.build()
                    );
                }
            }
        }
    }

    private static Logger log = LogManager.getLogger();
}
