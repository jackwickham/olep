package net.jackw.olep.edge;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.District;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.Order;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.Warehouse;
import net.jackw.olep.common.records.WarehouseSpecificKey;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLConnection implements AutoCloseable {
    private final Connection connection;

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
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    public void rollback() throws SQLException {
        connection.rollback();
    }

    public Warehouse loadWarehouse(int id) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery("SELECT * FROM WAREHOUSE WHERE W_ID=" + id)) {
            results.next();
            return new Warehouse(
                results.getInt("ID"), results.getString("NAME"), getAddress(results),
                results.getBigDecimal("TAX"), results.getBigDecimal("YTD")
            );
        }
    }

    public District loadDistrict(WarehouseSpecificKey key) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery("SELECT * FROM DISTRICT WHERE W_ID=" + key.warehouseId + " AND ID=" + key.id)) {
            results.next();
            return new District(
                results.getInt("ID"), results.getInt("W_ID"), results.getString("NAME"),
                getAddress(results), results.getBigDecimal("TAX"), results.getBigDecimal("YTD"),
                results.getInt("NEXT_O_ID")
            );
        }
    }

    public Customer loadCustomer(DistrictSpecificKey customer) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery("SELECT * FROM CUSTOMER WHERE W_ID=" + customer.warehouseId + "AND D_ID=" + customer.districtId + " AND ID=" + customer.id)) {
            results.next();
            return new Customer(
                new CustomerShared(
                    results.getInt("ID"), results.getInt("D_ID"), results.getInt("W_ID"),
                    results.getString("FIRST"), results.getString("MIDDLE"), results.getString("LAST"),
                    getAddress(results), results.getString("PHONE"), results.getLong("SINCE"),
                    Credit.fromByteValue(results.getByte("CREDIT")), results.getBigDecimal("CREDIT_LIMIT"),
                    results.getBigDecimal("DISCOUNT")
                ),
                new CustomerMutable(results.getBigDecimal("BALANCE"), results.getString("DATA"))
            );
        }
    }

    public Item loadItem(int id) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery("SELECT * FROM ITEM WHERE ID=" + id)) {
            results.next();
            if (results.isAfterLast()) {
                return null;
            }
            return new Item(
                results.getInt("ID"), results.getInt("IM_ID"), results.getString("NAME"),
                results.getBigDecimal("PRICE"), results.getString("DATA")
            );
        }
    }

    public Stock loadStock(WarehouseSpecificKey key) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery("SELECT * FROM STOCK WHERE W_ID=" + key.warehouseId + " AND I_ID=" + key.id)) {
            results.next();
            return new Stock(
                new StockShared(
                    results.getInt("I_ID"), results.getInt("W_ID"),
                    results.getString("DIST_01"), results.getString("DIST_02"), results.getString("DIST_03"),
                    results.getString("DIST_04"), results.getString("DIST_05"), results.getString("DIST_06"),
                    results.getString("DIST_07"), results.getString("DIST_08"), results.getString("DIST_09"),
                    results.getString("DIST_10"), results.getString("DATA")
                ),
                results.getInt("QUANTITY")
            );
        }
    }

    public void incrementDistrictNextOrderId(WarehouseSpecificKey district) throws SQLException {
        try (Statement statement = connection.createStatement()) {
             statement.executeUpdate("UPDATE DISTRICT SET NEXT_O_ID=NEXT_O_ID+1 WHERE W_ID=" + district.warehouseId + " AND ID=" + district.id);
        }
    }

    public void insertOrder(Order order) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO `ORDER` (ID, D_ID, W_ID, C_ID, ENTRY_D, CARRIER_ID, OL_CNT, ALL_LOCAL) VALUE " +
                "(" + order.orderId + "," + order.districtId + "," + order.warehouseId + "," + order.customerId + "," +
                order.entryDate + "," + (order.carrierId == null ? "NULL" : order.carrierId) + "," + order.orderLines.size() +
                "," + (order.allLocal ? 1 : 0) + ")");

            for (OrderLine line : order.orderLines) {
                statement.executeUpdate("INSERT INTO ORDER_LINE  (O_ID, D_ID, W_ID, `NUMBER`, I_ID, SUPPLY_W_ID, DELIVERY_D, QUANTITY, " +
                    "AMOUNT, DIST_INFO) VALUE (" + order.orderId + "," + order.districtId + "," + order.warehouseId + "," +
                    line.lineNumber + "," + line.itemId + "," + line.supplyWarehouseId + "," + (line.deliveryDate == null ? "NULL" : line.deliveryDate) +
                    "," + line.quantity + "," + line.amount + "," + line.distInfo + ")");
            }

            statement.executeUpdate("INSERT INTO NEW_ORDER (O_ID, D_ID, W_ID) VALUE (" + order.orderId + "," + order.districtId + "," + order.warehouseId + ")");
        }
    }

    private Address getAddress(ResultSet results) throws SQLException {
        return new Address(
            results.getString("STREET_1"), results.getString("STREET_2"),
            results.getString("CITY"), results.getString("STATE"), results.getString("ZIP")
        );
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

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
