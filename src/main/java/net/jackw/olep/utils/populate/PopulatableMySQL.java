package net.jackw.olep.utils.populate;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.edge.MySQLConnection;
import net.jackw.olep.message.modification.OrderLineModification;

import java.math.BigDecimal;
import java.sql.SQLException;

public class PopulatableMySQL implements PopulatableDatabase {
    private final MySQLConnection connection;

    public PopulatableMySQL(DatabaseConfig config) {
        try {
            connection = new MySQLConnection(config);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void populateItem(Item item) {
        try {
            connection.insertItem(item);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void populateWarehouse(WarehouseShared warehouse) {
        try {
            connection.insertWarehouse(warehouse, new BigDecimal("300000.00"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void populateDistrict(DistrictShared district) {
        try {
            connection.insertDistrict(district, new BigDecimal("30000.00"), 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void populateCustomer(Customer customer) {
        try {
            connection.insertCustomer(customer, new BigDecimal("10.00"), 1, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void populateDeliveredOrder(OrderFactory.DeliveredOrder order) {
        try {
            connection.insertOrder(
                order.newOrderModification.orderId, order.newOrderModification.districtId, order.newOrderModification.warehouseId,
                order.newOrderModification.customerId, order.newOrderModification.date, order.deliveryModification.carrierId,
                order.newOrderModification.lines.size(), true
            );
            for (OrderLineModification line : order.newOrderModification.lines) {
                connection.insertOrderLine(
                    order.newOrderModification.orderId, order.newOrderModification.districtId,
                    order.newOrderModification.warehouseId, line.lineNumber, line.itemId, line.supplyWarehouseId,
                    line.deliveryDate, line.quantity, line.amount, line.distInfo
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void populateUndeliveredOrder(OrderFactory.UndeliveredOrder order) {
        try {
            connection.insertOrder(
                order.newOrderModification.orderId,
                order.newOrderModification.districtId,
                order.newOrderModification.warehouseId,
                order.newOrderModification.customerId,
                order.newOrderModification.date,
                null,
                order.newOrderModification.lines.size(),
                true
            );
            for (OrderLineModification line : order.newOrderModification.lines) {
                connection.insertOrderLine(
                    order.newOrderModification.orderId, order.newOrderModification.districtId,
                    order.newOrderModification.warehouseId, line.lineNumber, line.itemId, line.supplyWarehouseId,
                    line.deliveryDate, line.quantity, line.amount, line.distInfo
                );
            }
            connection.insertNewOrder(order.newOrderModification.orderId, order.newOrderModification.districtId, order.newOrderModification.warehouseId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finaliseDistrict(DistrictShared district, int nextOrderId) {
        try {
            connection.setDistrictNextOrderId(district.getKey(), nextOrderId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void populateStock(Stock stock) {
        try {
            connection.insertStock(stock, 0, 0, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
