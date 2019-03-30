package net.jackw.olep.edge;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

@SuppressWarnings("Duplicates")
public class MySQLConnection implements AutoCloseable {
    private final Connection connection;

    private final PreparedStatement loadWarehouseStatement;
    private final PreparedStatement loadDistrictStatement;
    private final PreparedStatement loadCustomerStatement;
    private final PreparedStatement loadCustomerByNameStatement;
    private final PreparedStatement loadItemStatement;
    private final PreparedStatement loadStockStatement;
    private final PreparedStatement loadLatestNewOrderStatement;
    private final PreparedStatement getOrderLineAmountTotalStatement;
    private final PreparedStatement getStockLevelStatement;

    private final PreparedStatement incrementNextOrderIdStatement;
    private final PreparedStatement updateStockStatement;
    private final PreparedStatement setWarehouseYtdStatement;
    private final PreparedStatement setDistrictYtdStatement;
    private final PreparedStatement paymentUpdateCustomerStatement;
    private final PreparedStatement setOrderCarrierIdStatement;
    private final PreparedStatement setOrderLineDeliveryDateStatement;
    private final PreparedStatement deliveryUpdateCustomerStatement;

    private final PreparedStatement insertOrderStatement;
    private final PreparedStatement insertOrderLineStatement;
    private final PreparedStatement insertNewOrderStatement;
    private final PreparedStatement insertHistoryStatement;

    private final PreparedStatement insertWarehouseStatement;
    private final PreparedStatement insertDistrictStatement;
    private final PreparedStatement insertCustomerStatement;
    private final PreparedStatement insertItemStatement;
    private final PreparedStatement insertStockStatement;

    private final PreparedStatement deleteNewOrderStatement;

    public MySQLConnection(DatabaseConfig config) throws SQLException {
        String url = "jdbc:mysql://" + config.getMysqlServer() + "/";
        connection = DriverManager.getConnection(url, config.getMysqlUser(), config.getMysqlPassword());

        try (Statement createDatabaseStatement = connection.createStatement()) {
            createDatabaseStatement.execute("CREATE DATABASE IF NOT EXISTS olep");
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute("USE olep");
        }
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        loadWarehouseStatement = connection.prepareStatement("SELECT * FROM WAREHOUSE WHERE W_ID=?");
        loadDistrictStatement = connection.prepareStatement("SELECT * FROM DISTRICT WHERE D_W_ID=? AND D_ID=?");
        loadCustomerStatement = connection.prepareStatement("SELECT * FROM CUSTOMER WHERE C_W_ID=? AND C_D_ID=? AND C_ID=?");
        loadCustomerByNameStatement = connection.prepareStatement("SELECT * FROM CUSTOMER WHERE C_W_ID=? AND C_D_ID=? AND C_LAST=? ORDER BY C_FIRST ASC", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        loadItemStatement = connection.prepareStatement("SELECT * FROM ITEM WHERE I_ID=?");
        loadStockStatement = connection.prepareStatement("SELECT * FROM STOCK WHERE S_W_ID=? AND S_I_ID=?");
        loadLatestNewOrderStatement = connection.prepareStatement("SELECT * FROM NEW_ORDER INNER JOIN `ORDER` ON O_W_ID=NO_W_ID AND O_D_ID=NO_D_ID WHERE NO_W_ID=? AND NO_D_ID=? LIMIT 1");
        getOrderLineAmountTotalStatement = connection.prepareStatement("SELECT SUM(OL_AMOUNT) AS total FROM ORDER_LINE WHERE OL_W_ID=? AND OL_D_ID=? AND OL_O_ID=?");
        getStockLevelStatement = connection.prepareStatement("SELECT COUNT(*) as count FROM `ORDER` INNER JOIN `ORDER_LINE` ON O_ID=OL_O_ID AND O_W_ID=OL_W_ID AND O_D_ID=OL_D_ID " +
            "INNER JOIN STOCK ON OL_I_ID=S_I_ID AND S_W_ID=OL_W_ID WHERE O_W_ID=? AND O_D_ID=? AND O_ID >= (SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=? AND D_ID=?) - 20 AND S_QUANTITY < ?");

        incrementNextOrderIdStatement = connection.prepareStatement("UPDATE DISTRICT SET D_NEXT_O_ID=D_NEXT_O_ID+1 WHERE D_W_ID=? AND D_ID=?");
        updateStockStatement = connection.prepareStatement("UPDATE STOCK SET S_QUANTITY=?, S_YTD=?, S_ORDER_CNT=?, S_REMOTE_CNT=? WHERE S_W_ID=? AND S_I_ID=?");
        setWarehouseYtdStatement = connection.prepareStatement("UPDATE WAREHOUSE SET W_YTD=? WHERE W_ID=?");
        setDistrictYtdStatement = connection.prepareStatement("UPDATE DISTRICT SET D_YTD=? WHERE D_W_ID=? AND D_ID=?");
        paymentUpdateCustomerStatement = connection.prepareStatement("UPDATE CUSTOMER SET C_BALANCE=?, C_YTD_PAYMENT=?, C_PAYMENT_CNT=?, C_DATA=? WHERE C_W_ID=? AND C_D_ID=? AND C_ID=?");
        setOrderCarrierIdStatement = connection.prepareStatement("UPDATE `ORDER` SET O_CARRIER_ID=? WHERE O_W_ID=? AND O_D_ID=? AND O_ID=?");
        setOrderLineDeliveryDateStatement = connection.prepareStatement("UPDATE ORDER_LINE SET OL_DELIVERY_D=? WHERE OL_W_ID=? AND OL_D_ID=? AND OL_O_ID=?");
        deliveryUpdateCustomerStatement = connection.prepareStatement("UPDATE CUSTOMER SET C_BALANCE=C_BALANCE+?, C_DELIVERY_CNT=C_DELIVERY_CNT+1 WHERE C_W_ID=? AND C_D_ID=? AND C_ID=?");

        insertOrderStatement = connection.prepareStatement("INSERT INTO `ORDER` (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUE (?, ?, ?, ?, ?, ?, ?, ?)");
        insertOrderLineStatement = connection.prepareStatement("INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        insertNewOrderStatement = connection.prepareStatement("INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUE (?, ?, ?)");
        insertHistoryStatement = connection.prepareStatement("INSERT INTO HISTORY (H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUE (?, ?, ?, ?, ?, ?, ?)");

        insertWarehouseStatement = connection.prepareStatement("INSERT INTO WAREHOUSE (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        insertDistrictStatement = connection.prepareStatement("INSERT INTO DISTRICT (D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        insertCustomerStatement = connection.prepareStatement("INSERT INTO CUSTOMER (C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE," +
            "C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT,  C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        insertItemStatement = connection.prepareStatement("INSERT INTO ITEM (I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA) VALUES (?, ?, ?, ?, ?)");
        insertStockStatement = connection.prepareStatement("INSERT INTO STOCK (S_W_ID, S_I_ID, S_QUANTITY, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10," +
            "S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        deleteNewOrderStatement = connection.prepareStatement("DELETE FROM NEW_ORDER WHERE NO_W_ID=? AND NO_D_ID=? AND NO_O_ID=?");
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    public void rollback() throws SQLException {
        connection.rollback();
    }

    ////// Loads /////

    @MustBeClosed
    public ResultSet loadWarehouse(int id) throws SQLException {
        loadWarehouseStatement.setInt(1, id);
        ResultSet results = loadWarehouseStatement.executeQuery();
        results.next();
        return results;
    }

    @MustBeClosed
    public ResultSet loadDistrict(WarehouseSpecificKey key) throws SQLException {
        loadDistrictStatement.setInt(1, key.warehouseId);
        loadDistrictStatement.setInt(2, key.id);
        ResultSet results = loadDistrictStatement.executeQuery();
        results.next();
        return results;
    }

    @MustBeClosed
    public ResultSet loadCustomer(DistrictSpecificKey customer) throws SQLException {
        loadCustomerStatement.setInt(1, customer.warehouseId);
        loadCustomerStatement.setInt(2, customer.districtId);
        loadCustomerStatement.setInt(3, customer.id);
        ResultSet results = loadCustomerStatement.executeQuery();
        results.next();
        return results;
    }

    @MustBeClosed
    public ResultSet loadCustomersByName(CustomerNameKey customer) throws SQLException {
        loadCustomerByNameStatement.setInt(1, customer.warehouseId);
        loadCustomerByNameStatement.setInt(2, customer.districtId);
        loadCustomerByNameStatement.setString(3, customer.lastName);
        return loadCustomerByNameStatement.executeQuery();
    }

    @MustBeClosed
    public ResultSet loadItem(int id) throws SQLException {
        loadItemStatement.setInt(1, id);
        ResultSet results = loadItemStatement.executeQuery();
        results.next();
        return results;
    }

    @MustBeClosed
    public ResultSet loadStock(WarehouseSpecificKey key) throws SQLException {
        loadStockStatement.setInt(1, key.warehouseId);
        loadStockStatement.setInt(2, key.id);
        ResultSet results = loadStockStatement.executeQuery();
        results.next();
        return results;
    }

    @MustBeClosed
    public ResultSet loadLatestNewOrder(int districtId, int warehouseId) throws SQLException {
        loadLatestNewOrderStatement.setInt(1, warehouseId);
        loadLatestNewOrderStatement.setInt(2, districtId);
        return loadLatestNewOrderStatement.executeQuery();
    }

    public BigDecimal getOrderLineAmountTotal(int orderId, int districtId, int warehouseId) throws SQLException {
        int index = 0;
        getOrderLineAmountTotalStatement.setInt(++index, warehouseId);
        getOrderLineAmountTotalStatement.setInt(++index, districtId);
        getOrderLineAmountTotalStatement.setInt(++index, orderId);
        ResultSet results = getOrderLineAmountTotalStatement.executeQuery();
        results.next();
        return results.getBigDecimal("total");
    }

    public int getStockLevel(int districtId, int warehouseId, int threshold) throws SQLException {
        int index = 0;
        getStockLevelStatement.setInt(++index, warehouseId);
        getStockLevelStatement.setInt(++index, districtId);
        getStockLevelStatement.setInt(++index, warehouseId);
        getStockLevelStatement.setInt(++index, districtId);
        getStockLevelStatement.setInt(++index, threshold);

        ResultSet results = getStockLevelStatement.executeQuery();
        results.next();
        return results.getInt("count");
    }

    ///// Updates /////

    public void incrementDistrictNextOrderId(WarehouseSpecificKey district) throws SQLException {
        incrementNextOrderIdStatement.setInt(1, district.warehouseId);
        incrementNextOrderIdStatement.setInt(2, district.id);
        incrementNextOrderIdStatement.executeUpdate();
    }

    public void updateStock(WarehouseSpecificKey key, int newQuantity, int newYtd, int newOrderCnt, int newRemoteCnt) throws SQLException {
        updateStockStatement.setInt(1, newQuantity);
        updateStockStatement.setInt(2, newYtd);
        updateStockStatement.setInt(3, newOrderCnt);
        updateStockStatement.setInt(4, newRemoteCnt);
        updateStockStatement.setInt(5, key.warehouseId);
        updateStockStatement.setInt(6, key.id);
        updateStockStatement.executeUpdate();
    }

    public void setWarehouseYtd(int id, int newYtd) throws SQLException {
        setWarehouseYtdStatement.setInt(1, newYtd);
        setWarehouseYtdStatement.setInt(2, id);
        setWarehouseYtdStatement.executeUpdate();
    }

    public void setDistrictYtd(int districtId, int warehouseId, int newYtd) throws SQLException {
        setDistrictYtdStatement.setInt(1, newYtd);
        setDistrictYtdStatement.setInt(2, warehouseId);
        setDistrictYtdStatement.setInt(3, districtId);
        setDistrictYtdStatement.executeUpdate();
    }

    public void paymentUpdateCustomer(int id, int districtId, int warehouseId, BigDecimal balance, BigDecimal ytdPayment, int paymentCount, String data) throws SQLException {
        int index = 0;
        paymentUpdateCustomerStatement.setBigDecimal(++index, balance);
        paymentUpdateCustomerStatement.setBigDecimal(++index, ytdPayment);
        paymentUpdateCustomerStatement.setInt(++index, paymentCount);
        paymentUpdateCustomerStatement.setString(++index, data);
        paymentUpdateCustomerStatement.setInt(++index, warehouseId);
        paymentUpdateCustomerStatement.setInt(++index, districtId);
        paymentUpdateCustomerStatement.setInt(++index, id);
        paymentUpdateCustomerStatement.executeUpdate();
    }

    public void setOrderCarrierId(int orderId, int districtId, int warehouseId, int carrierId) throws SQLException {
        int index = 0;
        setOrderCarrierIdStatement.setInt(++index, carrierId);
        setOrderCarrierIdStatement.setInt(++index, warehouseId);
        setOrderCarrierIdStatement.setInt(++index, districtId);
        setOrderCarrierIdStatement.setInt(++index, orderId);
        setOrderCarrierIdStatement.executeUpdate();
    }

    public void setOrderLineDeliveryDate(int orderId, int districtId, int warehouseId, long deliveryDate) throws SQLException {
        int index = 0;
        setOrderLineDeliveryDateStatement.setLong(++index, deliveryDate);
        setOrderLineDeliveryDateStatement.setInt(++index, warehouseId);
        setOrderLineDeliveryDateStatement.setInt(++index, districtId);
        setOrderLineDeliveryDateStatement.setInt(++index, orderId);
        setOrderLineDeliveryDateStatement.executeUpdate();
    }

    public void deliveryUpdateCustomer(int customerId, int districtId, int warehouseId, BigDecimal totalAmount) throws SQLException {
        int index = 0;
        deliveryUpdateCustomerStatement.setBigDecimal(++index, totalAmount);
        deliveryUpdateCustomerStatement.setInt(++index, warehouseId);
        deliveryUpdateCustomerStatement.setInt(++index, districtId);
        deliveryUpdateCustomerStatement.setInt(++index, customerId);
        deliveryUpdateCustomerStatement.executeUpdate();
    }

    ///// Inserts /////

    public void insertOrder(int id, int districtId, int warehouseId, int customerId, long entryDate, Integer carrierId, int olCount, boolean allLocal) throws SQLException {
        insertOrderStatement.setInt(1, id);
        insertOrderStatement.setInt(2, districtId);
        insertOrderStatement.setInt(3, warehouseId);
        insertOrderStatement.setInt(4, customerId);
        insertOrderStatement.setLong(5, entryDate);
        if (carrierId == null) {
            insertOrderStatement.setNull(6, Types.TINYINT);
        } else {
            insertOrderStatement.setInt(6, carrierId);
        }
        insertOrderStatement.setInt(7, olCount);
        insertOrderStatement.setBoolean(8, allLocal);
        insertOrderStatement.executeUpdate();
    }

    public void insertOrderLine(int orderId, int districtId, int warehouseId, int lineNumber, int itemId, int supplyWarehouseId, Long deliveryDate, int quantity, BigDecimal amount, String distInfo) throws SQLException {
        insertOrderLineStatement.setInt(1, orderId);
        insertOrderLineStatement.setInt(2, districtId);
        insertOrderLineStatement.setInt(3, warehouseId);
        insertOrderLineStatement.setInt(4, lineNumber);
        insertOrderLineStatement.setInt(5, itemId);
        insertOrderLineStatement.setInt(6, supplyWarehouseId);
        if (deliveryDate == null) {
            insertOrderLineStatement.setNull(7, Types.BIGINT);
        } else {
            insertOrderLineStatement.setLong(7, deliveryDate);
        }
        insertOrderLineStatement.setInt(8, quantity);
        insertOrderLineStatement.setBigDecimal(9, amount);
        insertOrderLineStatement.setString(10, distInfo);
        insertOrderLineStatement.executeUpdate();
    }

    public void insertNewOrder(int orderId, int districtId, int warehouseId) throws SQLException {
        int index = 0;
        insertNewOrderStatement.setInt(++index, orderId);
        insertNewOrderStatement.setInt(++index, districtId);
        insertNewOrderStatement.setInt(++index, warehouseId);
        insertNewOrderStatement.executeUpdate();
    }

    public void insertHistory(int customerDistrictId, int customerWarehouseId, int districtId, int warehouseId, long date, BigDecimal amount, String data) throws SQLException {
        int index = 0;
        insertHistoryStatement.setInt(++index, customerDistrictId);
        insertHistoryStatement.setInt(++index, customerWarehouseId);
        insertHistoryStatement.setInt(++index, districtId);
        insertHistoryStatement.setInt(++index, warehouseId);
        insertHistoryStatement.setLong(++index, date);
        insertHistoryStatement.setBigDecimal(++index, amount);
        insertHistoryStatement.setString(++index, data);
        insertHistoryStatement.executeUpdate();
    }

    public void insertWarehouse(WarehouseShared warehouse, BigDecimal ytd) throws SQLException {
        int index = 0;
        insertWarehouseStatement.setInt(++index, warehouse.id);
        insertWarehouseStatement.setString(++index, warehouse.name);
        index = bindAddress(insertWarehouseStatement, warehouse.address, index);
        insertWarehouseStatement.setBigDecimal(++index, warehouse.tax);
        insertWarehouseStatement.setBigDecimal(++index, ytd);
        insertWarehouseStatement.executeUpdate();
    }

    public void insertDistrict(DistrictShared district, BigDecimal ytd, int nextOrderId) throws SQLException {
        int index = 0;
        insertDistrictStatement.setInt(++index, district.id);
        insertDistrictStatement.setInt(++index, district.warehouseId);
        insertDistrictStatement.setString(++index, district.name);
        index = bindAddress(insertDistrictStatement, district.address, index);
        insertDistrictStatement.setBigDecimal(++index, district.tax);
        insertDistrictStatement.setBigDecimal(++index, ytd);
        insertDistrictStatement.setInt(++index, nextOrderId);
        insertDistrictStatement.executeUpdate();
    }

    public void insertCustomer(Customer customer, BigDecimal ytdPayment, int paymentCount, int deliveryCount) throws SQLException {
        int index = 0;
        insertCustomerStatement.setInt(++index, customer.customerShared.id);
        insertCustomerStatement.setInt(++index, customer.customerShared.districtId);
        insertCustomerStatement.setInt(++index, customer.customerShared.warehouseId);
        insertCustomerStatement.setString(++index, customer.customerShared.firstName);
        insertCustomerStatement.setString(++index, customer.customerShared.middleName);
        insertCustomerStatement.setString(++index, customer.customerShared.lastName);
        index = bindAddress(insertCustomerStatement, customer.customerShared.address, index);
        insertCustomerStatement.setString(++index, customer.customerShared.phone);
        insertCustomerStatement.setLong(++index, customer.customerShared.since);
        insertCustomerStatement.setByte(++index, customer.customerShared.credit.getByteValue());
        insertCustomerStatement.setBigDecimal(++index, customer.customerShared.creditLimit);
        insertCustomerStatement.setBigDecimal(++index, customer.customerShared.discount);
        insertCustomerStatement.setBigDecimal(++index, customer.customerMutable.balance);
        insertCustomerStatement.setBigDecimal(++index, ytdPayment); // default 10.00
        insertCustomerStatement.setInt(++index, paymentCount); // default 1
        insertCustomerStatement.setInt(++index, deliveryCount); // default 0
        insertCustomerStatement.setString(++index, customer.customerMutable.data);

        insertCustomerStatement.executeUpdate();
    }

    public void insertItem(Item item) throws SQLException {
        int index = 0;
        insertItemStatement.setInt(++index, item.id);
        insertItemStatement.setInt(++index, item.imageId);
        insertItemStatement.setString(++index, item.name);
        insertItemStatement.setBigDecimal(++index, item.price);
        insertItemStatement.setString(++index, item.data);

        insertItemStatement.executeUpdate();
    }

    public void insertStock(Stock stock, int ytd, int orderCnt, int remoteCnt) throws SQLException {
        int index = 0;
        insertStockStatement.setInt(++index, stock.stockShared.warehouseId);
        insertStockStatement.setInt(++index, stock.stockShared.itemId);
        insertStockStatement.setInt(++index, stock.stockQuantity);
        insertStockStatement.setString(++index, stock.stockShared.dist01);
        insertStockStatement.setString(++index, stock.stockShared.dist02);
        insertStockStatement.setString(++index, stock.stockShared.dist03);
        insertStockStatement.setString(++index, stock.stockShared.dist04);
        insertStockStatement.setString(++index, stock.stockShared.dist05);
        insertStockStatement.setString(++index, stock.stockShared.dist06);
        insertStockStatement.setString(++index, stock.stockShared.dist07);
        insertStockStatement.setString(++index, stock.stockShared.dist08);
        insertStockStatement.setString(++index, stock.stockShared.dist09);
        insertStockStatement.setString(++index, stock.stockShared.dist10);
        insertStockStatement.setInt(++index, ytd);
        insertStockStatement.setInt(++index, orderCnt);
        insertStockStatement.setInt(++index, remoteCnt);
        insertStockStatement.setString(++index, stock.stockShared.data);

        insertStockStatement.executeUpdate();
    }

    ///// Deletes /////

    public void deleteNewOrder(int orderId, int districtId, int warehouseId) throws SQLException {
        int index = 0;
        deleteNewOrderStatement.setInt(++index, warehouseId);
        deleteNewOrderStatement.setInt(++index, districtId);
        deleteNewOrderStatement.setInt(++index, orderId);
        deleteNewOrderStatement.executeUpdate();
    }

    //// Utilities /////

    private int bindAddress(PreparedStatement statement, Address address, int offset) throws SQLException {
        statement.setString(++offset, address.street1);
        statement.setString(++offset, address.street2);
        statement.setString(++offset, address.city);
        statement.setString(++offset, address.state);
        statement.setString(++offset, address.zip);
        return offset;
    }

    public Address getAddress(ResultSet results, String tablePrefix) throws SQLException {
        return new Address(
            results.getString(tablePrefix + "_STREET_1"),
            results.getString(tablePrefix + "_STREET_2"),
            results.getString(tablePrefix + "_CITY"),
            results.getString(tablePrefix + "_STATE"),
            results.getString(tablePrefix + "_ZIP")
        );
    }

    public void createTables() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, NEW_ORDER, `ORDER`, ORDER_LINE, ITEM, STOCK");

            statement.executeUpdate("CREATE TABLE WAREHOUSE (W_ID INTEGER, W_NAME VARCHAR(10), W_STREET_1 VARCHAR(20), W_STREET_2 VARCHAR(20), W_CITY VARCHAR(20), " +
                "W_STATE VARCHAR(2), W_ZIP VARCHAR(9), W_TAX DECIMAL(4, 4), W_YTD DECIMAL(12, 2), PRIMARY KEY (W_ID))");
            statement.executeUpdate("CREATE TABLE DISTRICT (D_ID INTEGER, D_W_ID INTEGER, D_NAME VARCHAR(10), D_STREET_1 VARCHAR(20)," +
                " D_STREET_2 VARCHAR(20), D_CITY VARCHAR(20), D_STATE VARCHAR(2), D_ZIP VARCHAR(9), D_TAX DECIMAL(4, 4), D_YTD DECIMAL(12, 2), D_NEXT_O_ID INTEGER, PRIMARY KEY (D_W_ID, D_ID))");
            statement.executeUpdate("CREATE TABLE CUSTOMER (C_ID INTEGER, C_D_ID INTEGER, C_W_ID INTEGER, C_FIRST VARCHAR(16)," +
                "C_MIDDLE VARCHAR(2), C_LAST VARCHAR(16), C_STREET_1 VARCHAR(20), C_STREET_2 VARCHAR(20), C_CITY VARCHAR(20), C_STATE VARCHAR(2), C_ZIP VARCHAR(9)," +
                "C_PHONE VARCHAR(16), C_SINCE BIGINT, C_CREDIT TINYINT, C_CREDIT_LIM DECIMAL(12, 2), C_DISCOUNT DECIMAL(4, 4), C_BALANCE DECIMAL(12, 2), " +
                "C_YTD_PAYMENT DECIMAL(12, 2), C_PAYMENT_CNT INT, C_DELIVERY_CNT INT, C_DATA VARCHAR(500), PRIMARY KEY (C_W_ID, C_D_ID, C_ID))");
            statement.executeUpdate("CREATE INDEX LAST ON CUSTOMER (C_LAST)");
            statement.executeUpdate("CREATE TABLE HISTORY (H_C_ID INT, H_C_D_ID INT, H_C_W_ID INT, H_D_ID INT, H_W_ID INT, H_DATE BIGINT, H_AMOUNT DEC(6, 2), H_DATA VARCHAR(24))");
            statement.executeUpdate("CREATE TABLE NEW_ORDER (NO_O_ID INT, NO_D_ID INT, NO_W_ID INT, PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID))");
            statement.executeUpdate("CREATE TABLE `ORDER` (O_ID INT, O_D_ID INT, O_W_ID INT, O_C_ID INT, O_ENTRY_D BIGINT," +
                "O_CARRIER_ID TINYINT NULL, O_OL_CNT TINYINT, O_ALL_LOCAL BOOL, PRIMARY KEY (O_W_ID, O_D_ID, O_ID))");
            statement.executeUpdate("CREATE TABLE ORDER_LINE (OL_O_ID INT, OL_D_ID INT, OL_W_ID INT, OL_NUMBER INT," +
                "OL_I_ID INT, OL_SUPPLY_W_ID INT, OL_DELIVERY_D BIGINT NULL, OL_QUANTITY TINYINT, OL_AMOUNT DEC(6, 2), OL_DIST_INFO VARCHAR(24), PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER))");
            statement.executeUpdate("CREATE TABLE ITEM (I_ID INT, I_IM_ID INT, I_NAME VARCHAR(24), I_PRICE DEC(5, 2), I_DATA VARCHAR(50), PRIMARY KEY (I_ID))");
            statement.executeUpdate("CREATE TABLE STOCK (S_I_ID INT, S_W_ID INT, S_QUANTITY INT, S_DIST_01 VARCHAR(24), S_DIST_02 VARCHAR(24)," +
                "S_DIST_03 VARCHAR(24), S_DIST_04 VARCHAR(24), S_DIST_05 VARCHAR(24), S_DIST_06 VARCHAR(24), S_DIST_07 VARCHAR(24), S_DIST_08 VARCHAR(24)," +
                "S_DIST_09 VARCHAR(24), S_DIST_10 VARCHAR(24), S_YTD INT, S_ORDER_CNT INT, S_REMOTE_CNT INT, S_DATA VARCHAR(50), PRIMARY KEY (S_W_ID, S_I_ID))");
        }
    }

    @MustBeClosed
    public Statement getStatement() throws SQLException {
        return connection.createStatement();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
