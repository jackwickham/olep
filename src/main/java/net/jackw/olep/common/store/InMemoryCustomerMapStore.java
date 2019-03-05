package net.jackw.olep.common.store;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class InMemoryCustomerMapStore implements WritableCustomerStore {
    // Store id -> Customer
    private Map<DistrictSpecificKey, CustomerShared> idMap;
    // Store lastName -> All customers with that last name (+district/warehouse)
    private Multimap<CustomerNameKey, CustomerShared> nameMultimap;

    InMemoryCustomerMapStore(int initialCapacity) {
        idMap = new HashMap<>(initialCapacity);
        nameMultimap = HashMultimap.create(initialCapacity / 10, 10);
    }

    @Override
    public void put(DistrictSpecificKey key, @Nonnull CustomerShared value) {
        nameMultimap.put(value.getNameKey(), value);
        idMap.put(key, value);
    }

    @Nullable
    @CanIgnoreReturnValue
    @Override
    public CustomerShared remove(DistrictSpecificKey key) {
        CustomerShared value = idMap.remove(key);
        if (value != null) {
            nameMultimap.remove(value.getNameKey(), value);
        }
        return value;
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


    //// Customer-specific methods ////


    private Ordering<CustomerShared> nameOrdering = new Ordering<>() {
        @Override
        public int compare(CustomerShared left, CustomerShared right) {
            return left.firstName.compareTo(right.firstName);
        }
    };

    @Nullable
    @Override
    public CustomerShared get(CustomerNameKey key) {
        Collection<CustomerShared> customers = nameMultimap.get(key);
        if (customers.isEmpty()) {
            return null;
        }
        // Need to return customer in 1-indexed position ceil(n/2) when sorted by first name
        // If there are 9 customers, we need to get the customer at index 4 (= half way)
        // If there are 10 customers, we need to get the customer at index 5 (= rounding up half way)
        // This means we need to trim the list to 5 (respectively 6) elements and sort those, then get the last element
        int cutoff = customers.size() / 2 + 1;
        return nameOrdering.leastOf(customers, cutoff).get(cutoff - 1);
    }

    @Override
    public void clear() {
        idMap.clear();
        nameMultimap.clear();
    }
}
