package net.jackw.olep.common.store;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.CustomerNameKeySerde;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.CustomerSharedSerde;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.DistrictSpecificKeySerde;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.utils.populate.PredictableCustomerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

public class DiskBackedCustomerMapStore implements WritableCustomerStore, AutoCloseable {
    private DatabaseConfig config;

    /**
     * Map of warehouse id + district id + customer id -> customer
     */
    private DiskBackedMapStore<DistrictSpecificKey, CustomerShared> idMap;

    /**
     * Map of warehouse id + district id + customer name -> customer
     *
     * The customer stored in this map is the one that TPC-C wants when looking a customer up by name, which is the
     * middle customer sorted by first name
     */
    private DiskBackedMapStore<CustomerNameKey, CustomerShared> nameMap;

    /**
     * The district that the current customer insertions correspond to. This assumes that customers are being inserted
     * grouped by district, which is currently the ase
     */
    private WarehouseSpecificKey currentPopulatingDistrict = null;

    /**
     * For the current populating district, a multimap of name to customer records with that name
     */
    private Multimap<String, CustomerShared> currentWarehouseNamesMultimap;

    /**
     * Create a new view on the store
     */
    public DiskBackedCustomerMapStore(DatabaseConfig config) {
        this.config = config;

        int capacity = config.getWarehouseCount() * config.getDistrictsPerWarehouse() * config.getCustomersPerDistrict();
        CustomerShared sampleCustomer = PredictableCustomerFactory.instanceFor(1, 1, 10).getCustomerShared(1);

        idMap = DiskBackedMapStore.create(
            capacity, DistrictSpecificKey.class, CustomerShared.class, "customer-id", sampleCustomer.getKey(),
            sampleCustomer, config, DistrictSpecificKeySerde.getInstance(), CustomerSharedSerde.getInstance()
        );
        nameMap = DiskBackedMapStore.create(
            capacity, CustomerNameKey.class, CustomerShared.class, "customer-name",
            new CustomerNameKey(sampleCustomer.lastName, 1, 1), sampleCustomer, config,
            CustomerNameKeySerde.getInstance(), CustomerSharedSerde.getInstance()
        );
    }

    @Nullable
    @Override
    public CustomerShared get(CustomerNameKey key) {
        return nameMap.get(key);
    }

    private Ordering<CustomerShared> nameOrdering = new Ordering<>() {
        @Override
        public int compare(CustomerShared left, CustomerShared right) {
            return left.firstName.compareTo(right.firstName);
        }
    };

    @Override
    public void put(DistrictSpecificKey key, @Nonnull CustomerShared value) {
        // Update the name map
        WarehouseSpecificKey district = new WarehouseSpecificKey(key.districtId, key.warehouseId);
        if (!district.equals(currentPopulatingDistrict)) {
            currentPopulatingDistrict = district;
            currentWarehouseNamesMultimap = HashMultimap.create(
                config.getCustomerNameRange(),
                config.getCustomersPerDistrict() / config.getCustomerNameRange()
            );
        }
        currentWarehouseNamesMultimap.put(value.lastName, value);

        Collection<CustomerShared> customersWithName = currentWarehouseNamesMultimap.get(value.lastName);
        // The name map should reference the customer in 1-indexed position ceil(n/2) when sorted by first name
        // If there are 9 customers, we need to get the customer at index 4 (= half way)
        // If there are 10 customers, we need to get the customer at index 5 (= rounding up half way)
        // This means we need to trim the list to 5 (respectively 6) elements and sort those, then get the last element
        int cutoff = customersWithName.size() / 2 + 1;
        CustomerShared middleCustomer = nameOrdering.leastOf(customersWithName, cutoff).get(cutoff - 1);
        nameMap.put(new CustomerNameKey(middleCustomer.lastName, middleCustomer.districtId, middleCustomer.warehouseId), middleCustomer);

        // Also update the ID map, and return the old value
        idMap.put(key, value);
    }

    @Nullable
    @Override
    public CustomerShared remove(DistrictSpecificKey key) {
        throw new UnsupportedOperationException("Can't remove from the customer map");
    }

    @Override
    public boolean containsKey(DistrictSpecificKey key) {
        return idMap.containsKey(key);
    }

    @Nullable
    @Override
    public CustomerShared get(DistrictSpecificKey key) {
        return idMap.get(key);
    }

    @Override
    public void clear() {
        idMap.clear();
        nameMap.clear();
        currentPopulatingDistrict = null;
        currentWarehouseNamesMultimap = null;
    }

    @Override
    public void close() {
        idMap.close();
        nameMap.close();
    }
}
