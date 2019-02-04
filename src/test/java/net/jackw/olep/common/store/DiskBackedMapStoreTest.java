package net.jackw.olep.common.store;

import org.junit.After;

public class DiskBackedMapStoreTest extends BaseMapStoreTest {
    public DiskBackedMapStoreTest() {
        super(DiskBackedMapStore.createIntegerKeyed(2, Val.class, "TestStore", new Val(0)));
    }

    @After
    public void closeMap() {
        ((DiskBackedMapStore) store).close();
    }
}
