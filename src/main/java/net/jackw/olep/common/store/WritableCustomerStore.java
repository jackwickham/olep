package net.jackw.olep.common.store;

import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;

public interface WritableCustomerStore extends SharedCustomerStore, WritableKeyValueStore<DistrictSpecificKey, CustomerShared> {
}
