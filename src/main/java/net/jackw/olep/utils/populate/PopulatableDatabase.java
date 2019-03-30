package net.jackw.olep.utils.populate;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.WarehouseShared;

public interface PopulatableDatabase extends AutoCloseable {
    void populateItem(Item item);
    void populateWarehouse(WarehouseShared warehouse);
    void populateDistrict(DistrictShared district);
    void populateCustomer(Customer customer);
    void populateDeliveredOrder(OrderFactory.DeliveredOrder order);
    void populateUndeliveredOrder(OrderFactory.UndeliveredOrder order);
    void finaliseDistrict(DistrictShared district, int nextOrderId);
    void populateStock(Stock stock);

    @Override
    void close();

    public static PopulatableDatabase getInstance(DatabaseConfig config, boolean populateImmutableStores, boolean populateMutableStores) {
        if (config.isSqlBenchmark()) {
            return new PopulatableMySQL(config);
        } else {
            return new PopulatableEventDatabase(config, populateImmutableStores, populateMutableStores);
        }
    }
}
