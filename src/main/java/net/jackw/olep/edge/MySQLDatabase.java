package net.jackw.olep.edge;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ForOverride;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.Arguments;
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
import net.jackw.olep.utils.populate.PopulateStores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Date;
import java.util.List;

public class MySQLDatabase implements Database {
    // TODO: This needs to be cleanly closed
    private final ThreadLocal<MySQLConnection> connection;

    public MySQLDatabase(DatabaseConfig config) {
        this.connection = ThreadLocal.withInitial(() -> {
            try {
                return new MySQLConnection(config);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
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
        ImmutableMap.Builder<Integer, Integer> processedOrders = ImmutableMap.builderWithExpectedSize(10);
        try {
            for (int districtId = 1; districtId <= 10; districtId++) {
                Integer orderId = new DeliveryTransaction(warehouseId, districtId, carrierId).run();
                if (orderId != null) {
                    processedOrders.put(districtId, orderId);
                }
            }
            return toTransactionStatus(new DeliveryResult(warehouseId, carrierId, processedOrders.build()));
        } catch (TransactionRejectedException e) {
            throw new AssertionError("Delivery transaction should never be rejected", e);
        }
    }

    @Override
    public int stockLevel(int districtId, int warehouseId, int stockThreshold) {
        try {
            return new StockLevelTransaction(districtId, warehouseId, stockThreshold).run();
        } catch (TransactionRejectedException e) {
            throw new AssertionError("Stock level should never be rejected", e);
        }
    }

    @Override
    public OrderStatusResult orderStatus(int customerId, int districtId, int warehouseId) {
        try {
            return new OrderStatusTransaction(customerId, districtId, warehouseId).run();
        } catch (TransactionRejectedException e) {
            throw new AssertionError("Order Status should never be rejected", e);
        }
    }

    @Override
    public OrderStatusResult orderStatus(String customerLastName, int districtId, int warehouseId) {
        try {
            return new OrderStatusTransaction(customerLastName, districtId, warehouseId).run();
        } catch (TransactionRejectedException e) {
            throw new AssertionError("Order Status should never be rejected", e);
        }
    }

    @Override
    public void close() {
        try {
            connection.get().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private <T extends TransactionResultMessage> TransactionStatus<T> toTransactionStatus(T value) {
        return new TransactionStatus<>(
            0,
            Futures.immediateFuture(null),
            Futures.immediateFuture(null),
            Futures.immediateFuture(value)
        );
    }

    private <T extends TransactionResultMessage> TransactionStatus<T> execute(Transaction<T> transaction) {
        try {
            return toTransactionStatus(transaction.run());
        } catch (TransactionRejectedException e) {
            return new TransactionStatus<>(
                0,
                Futures.immediateFuture(null),
                Futures.immediateFailedFuture(e),
                SettableFuture.create()
            );
        }
    }

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    private ResultSet loadCustomerByName(String lastName, int districtId, int warehouseId) throws SQLException {
        ResultSet resultSet = connection.get().loadCustomersByName(new CustomerNameKey(lastName, districtId, warehouseId));
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

    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        Arguments arguments = new Arguments(args);
        MySQLDatabase db = new MySQLDatabase(arguments.getConfig());
        db.connection.get().createTables();
        db.connection.get().commit();

        try (PopulateStores populator = new PopulateStores(arguments.getConfig(), true, true)) {
            populator.populate();
        }

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

        db.delivery(1, 5);

        db.stockLevel(1, 1, 100);

        db.orderStatus(1, 1, 1);
        db.orderStatus("BARBARBAR", 1, 1);

        db.close();
    }

    private abstract class Transaction<T> {
        @ForOverride
        protected abstract T exec() throws SQLException, TransactionRejectedException;

        public T run() throws TransactionRejectedException {
            do {
                try {
                    T result = exec();
                    connection.get().commit();
                    return result;
                } catch (SQLTransientException e) {
                    // This will occur if there's an error during commit
                    // retry
                } catch (SQLException e) {
                    log.error(e);
                    RuntimeException re = new RuntimeException(e);
                    try {
                        connection.get().rollback();
                    } catch (SQLException e2) {
                        re.addSuppressed(e2);
                    }
                    throw re;
                } catch (TransactionRejectedException e) {
                    try {
                        connection.get().rollback();
                    } catch (SQLException se) {
                        RuntimeException re = new RuntimeException(se);
                        re.addSuppressed(e);
                        throw re;
                    }
                    throw e;
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
        protected NewOrderResult exec() throws SQLException, TransactionRejectedException {
            try (
                ResultSet wh = connection.get().loadWarehouse(warehouseId);
                ResultSet dist = connection.get().loadDistrict(new WarehouseSpecificKey(districtId, warehouseId));
                ResultSet cust = connection.get().loadCustomer(new DistrictSpecificKey(customerId, districtId, warehouseId));
            ) {
                int orderId = dist.getInt("D_NEXT_O_ID");

                connection.get().setDistrictNextOrderId(new WarehouseSpecificKey(districtId, warehouseId), orderId + 1);

                boolean allLocal = lines.stream().allMatch(line -> line.supplyingWarehouseId == warehouseId);
                long entryDate = new Date().getTime();

                connection.get().insertOrder(
                    orderId, districtId, warehouseId, customerId, entryDate, null, lines.size(), allLocal
                );
                connection.get().insertNewOrder(orderId, districtId, warehouseId);

                ImmutableList.Builder<OrderLineResult> lineResultBuilder = ImmutableList.builderWithExpectedSize(lines.size());

                int lineNumber = 0;
                for (NewOrderRequest.OrderLine line : lines) {
                    WarehouseSpecificKey stockKey = new WarehouseSpecificKey(line.itemId, line.supplyingWarehouseId);
                    try (
                        ResultSet item = connection.get().loadItem(line.itemId);
                        ResultSet stock = connection.get().loadStock(stockKey)
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
                        connection.get().updateStock(
                            stockKey, newStockQuantity, stock.getInt("S_YTD") + line.quantity,
                            stock.getInt("S_ORDER_CNT") + 1, newStockRemoteCount
                        );

                        BigDecimal lineAmount = item.getBigDecimal("I_PRICE").multiply(new BigDecimal(line.quantity));

                        connection.get().insertOrderLine(
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
        protected PaymentResult exec() throws SQLException {
            if (customerName == null) {
                try (ResultSet cust = connection.get().loadCustomer(new DistrictSpecificKey(customerId, customerDistrictId, customerWarehouseId))) {
                    return performPayment(cust);
                }
            } else {
                try (ResultSet cust = loadCustomerByName(customerName, customerDistrictId, customerWarehouseId)) {
                    return performPayment(cust);
                }
            }
        }

        protected PaymentResult performPayment(ResultSet customer) throws SQLException {
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

            connection.get().paymentUpdateCustomer(
                customerId, customerDistrictId, customerWarehouseId, customerBalance, customerYtdPayment,
                customerPaymentCount, customerData
            );

            try (
                ResultSet wh = connection.get().loadWarehouse(warehouseId);
                ResultSet dist = connection.get().loadDistrict(new WarehouseSpecificKey(districtId, warehouseId));
            ) {
                connection.get().setWarehouseYtd(warehouseId, wh.getInt("W_YTD") + 1);
                connection.get().setDistrictYtd(districtId, warehouseId, dist.getInt("D_YTD") + 1);

                String historyData = wh.getString("W_NAME") + "    " + dist.getString("D_NAME");
                connection.get().insertHistory(
                    customerDistrictId, customerWarehouseId, districtId, warehouseId, new Date().getTime(), amount, historyData
                );

                return new PaymentResult(
                    districtId, connection.get().getAddress(dist, "D"), warehouseId, connection.get().getAddress(wh, "W"),
                    customerId, customerDistrictId, customerWarehouseId, customer.getString("C_FIRST"),
                    customer.getString("C_MIDDLE"), customer.getString("C_LAST"),
                    connection.get().getAddress(customer, "C"), customer.getString("C_PHONE"),
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
        protected Integer exec() throws SQLException {
            try (ResultSet results = connection.get().loadLatestNewOrder(districtId, warehouseId)) {
                if (results.next()) {
                    int orderId = results.getInt("NO_O_ID");
                    connection.get().deleteNewOrder(orderId, districtId, warehouseId);
                    connection.get().setOrderCarrierId(orderId, districtId, warehouseId, carrierId);
                    connection.get().setOrderLineDeliveryDate(orderId, districtId, warehouseId, new Date().getTime());
                    BigDecimal amount = connection.get().getOrderLineAmountTotal(orderId, districtId, warehouseId);
                    connection.get().deliveryUpdateCustomer(results.getInt("O_C_ID"), districtId, warehouseId, amount);

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
        protected Integer exec() throws SQLException, TransactionRejectedException {
            return connection.get().getStockLevel(districtId, warehouseId, stockThreshold);
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
        private ResultSet getCustomer() throws SQLException {
            if (customerLastName != null) {
                return loadCustomerByName(customerLastName, districtId, warehouseId);
            } else {
                return connection.get().loadCustomer(new DistrictSpecificKey(customerId, districtId, warehouseId));
            }
        }

        @Override
        protected OrderStatusResult exec() throws SQLException, TransactionRejectedException {
            try (ResultSet customer = getCustomer()) {
                int customerId = customer.getInt("C_ID");
                try (ResultSet orderLines = connection.get().getLatestOrder(customerId, districtId, warehouseId)) {
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
