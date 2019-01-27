package net.jackw.olep.common.store;

public class DiskBackedCustomerMapStoreTest extends BaseCustomerStoreTest {
    public DiskBackedCustomerMapStoreTest() {
        super(new DiskBackedCustomerMapStore());
        store.clear();
    }
}
