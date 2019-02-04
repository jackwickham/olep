package net.jackw.olep.common.store;

import org.junit.After;

public class DiskBackedCustomerMapStoreTest extends BaseCustomerStoreTest {
    public DiskBackedCustomerMapStoreTest() {
        super(new DiskBackedCustomerMapStore());
    }

    @After
    public void closeMap() {
        ((DiskBackedCustomerMapStore) this.store).close();
    }
}
