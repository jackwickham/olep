package net.jackw.olep.edge;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
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

public class MySQLConnection implements AutoCloseable {
    private final Connection connection;

    private final PreparedStatement loadWarehouseStatement;
    private final PreparedStatement loadDistrictStatement;
    private final PreparedStatement loadCustomerStatement;
    private final PreparedStatement loadItemStatement;
    private final PreparedStatement loadStockStatement;

    private final PreparedStatement incrementNextOrderIdStatement;
    private final PreparedStatement updateStockStatement;

    private final PreparedStatement insertOrderStatement;
    private final PreparedStatement insertOrderLineStatement;

    private final PreparedStatement insertWarehouseStatement;
    private final PreparedStatement insertDistrictStatement;
    private final PreparedStatement insertCustomerStatement;

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

        loadWarehouseStatement = connection.prepareStatement("SELECT * FROM WAREHOUSE WHERE ID=?");
        loadDistrictStatement = connection.prepareStatement("SELECT * FROM DISTRICT WHERE W_ID=? AND ID=?");
        loadCustomerStatement = connection.prepareStatement("SELECT * FROM CUSTOMER WHERE W_ID=? AND D_ID=? AND ID=?");
        loadItemStatement = connection.prepareStatement("SELECT * FROM ITEM WHERE ID=?");
        loadStockStatement = connection.prepareStatement("SELECT * FROM STOCK WHERE W_ID=? AND I_ID=?");

        incrementNextOrderIdStatement = connection.prepareStatement("UPDATE DISTRICT SET NEXT_O_ID=NEXT_O_ID+1 WHERE W_ID=? AND ID=?");
        updateStockStatement = connection.prepareStatement("UPDATE STOCK SET QUANTITY=?, YTD=?, ORDER_CNT=?, REMOTE_CNT=? WHERE W_ID=? AND I_ID=?");

        insertOrderStatement = connection.prepareStatement("INSERT INTO `ORDER` (ID, D_ID, W_ID, C_ID, ENTRY_D, CARRIER_ID, OL_CNT, ALL_LOCAL) VALUE (?, ?, ?, ?, ?, ?, ?, ?)");
        insertOrderLineStatement = connection.prepareStatement("INSERT INTO ORDER_LINE (O_ID, D_ID, W_ID, NUMBER, I_ID, SUPPLY_W_ID, DELIVERY_D, QUANTITY, AMOUNT, DIST_INFO) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        insertWarehouseStatement = connection.prepareStatement("INSERT INTO WAREHOUSE (ID, NAME, STREET_1, STREET_2, CITY, STATE, ZIP, TAX, YTD) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        insertDistrictStatement = connection.prepareStatement("INSERT INTO DISTRICT (ID, W_ID, NAME, STREET_1, STREET_2, CITY, STATE, ZIP, TAX, YTD, NEXT_O_ID) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        insertCustomerStatement = connection.prepareStatement("INSERT INTO CUSTOMER (ID, D_ID, W_ID, FIRST, MIDDLE, LAST, STREET_1, STREET_2, CITY, STATE, ZIP, PHONE, SINCE," +
            "CREDIT, CREDIT_LIM, DISCOUNT, BALANCE, YTD_PAYMENT,  PAYMENT_CNT, DELIVERY_CNT, DATA) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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

    private int bindAddress(PreparedStatement statement, Address address, int offset) throws SQLException {
        statement.setString(++offset, address.street1);
        statement.setString(++offset, address.street2);
        statement.setString(++offset, address.city);
        statement.setString(++offset, address.state);
        statement.setString(++offset, address.zip);
        return offset;
    }

    public void createTables() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, NEW_ORDER, `ORDER`, ORDER_LINE, ITEM, STOCK");

            statement.executeUpdate("CREATE TABLE WAREHOUSE (ID INTEGER, NAME VARCHAR(10), STREET_1 VARCHAR(20), STREET_2 VARCHAR(20), CITY VARCHAR(20), " +
                "STATE VARCHAR(2), ZIP VARCHAR(9), TAX DECIMAL(4, 4), YTD DECIMAL(12, 2), PRIMARY KEY (ID))");
            statement.executeUpdate("CREATE TABLE DISTRICT (ID INTEGER, W_ID INTEGER, NAME VARCHAR(10), STREET_1 VARCHAR(20)," +
                " STREET_2 VARCHAR(20), CITY VARCHAR(20), STATE VARCHAR(2), ZIP VARCHAR(9), TAX DECIMAL(4, 4), YTD DECIMAL(12, 2), NEXT_O_ID INTEGER, PRIMARY KEY (W_ID, ID))");
            statement.executeUpdate("CREATE TABLE CUSTOMER (ID INTEGER, D_ID INTEGER, W_ID INTEGER, FIRST VARCHAR(16)," +
                "MIDDLE VARCHAR(2), LAST VARCHAR(16), STREET_1 VARCHAR(20), STREET_2 VARCHAR(20), CITY VARCHAR(20), STATE VARCHAR(2), ZIP VARCHAR(9)," +
                "PHONE VARCHAR(16), SINCE BIGINT, CREDIT TINYINT, CREDIT_LIM DECIMAL(12, 2), DISCOUNT DECIMAL(4, 4), BALANCE DECIMAL(12, 2), " +
                "YTD_PAYMENT DECIMAL(12, 2), PAYMENT_CNT INT, DELIVERY_CNT INT, DATA VARCHAR(500), PRIMARY KEY (W_ID, D_ID, ID))");
            statement.executeUpdate("CREATE INDEX LAST ON CUSTOMER (LAST)");
            statement.executeUpdate("CREATE TABLE HISTORY (C_ID INT, C_D_ID INT, C_W_ID INT, D_ID INT, W_ID INT, DATE BIGINT, AMOUNT DEC(6, 2), DATA VARCHAR(24))");
            statement.executeUpdate("CREATE TABLE NEW_ORDER (O_ID INT, D_ID INT, W_ID INT, PRIMARY KEY (W_ID, D_ID, O_ID))");
            statement.executeUpdate("CREATE TABLE `ORDER` (ID INT, D_ID INT, W_ID INT, C_ID INT, ENTRY_D BIGINT," +
                "CARRIER_ID TINYINT NULL, OL_CNT TINYINT, ALL_LOCAL BOOL, PRIMARY KEY (W_ID, D_ID, ID))");
            statement.executeUpdate("CREATE TABLE ORDER_LINE (O_ID INT, D_ID INT, W_ID INT, NUMBER INT," +
                "I_ID INT, SUPPLY_W_ID INT, DELIVERY_D BIGINT NULL, QUANTITY TINYINT, AMOUNT DEC(6, 2), DIST_INFO VARCHAR(24), PRIMARY KEY (W_ID, D_ID, O_ID, NUMBER))");
            statement.executeUpdate("CREATE TABLE ITEM (ID INT, IM_ID INT, NAME VARCHAR(24), PRICE DEC(5, 2), DATA VARCHAR(50), PRIMARY KEY (ID))");
            statement.executeUpdate("CREATE TABLE STOCK (I_ID INT, W_ID INT, QUANTITY INT, DIST_01 VARCHAR(24), DIST_02 VARCHAR(24)," +
                "DIST_03 VARCHAR(24), DIST_04 VARCHAR(24), DIST_05 VARCHAR(24), DIST_06 VARCHAR(24), DIST_07 VARCHAR(24), DIST_08 VARCHAR(24)," +
                "DIST_09 VARCHAR(24), DIST_10 VARCHAR(24), YTD INT, ORDER_CNT INT, REMOTE_CNT INT, DATA VARCHAR(50), PRIMARY KEY (W_ID, I_ID))");
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
