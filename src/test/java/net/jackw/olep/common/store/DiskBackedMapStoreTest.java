package net.jackw.olep.common.store;

import net.jackw.olep.common.DatabaseConfig;
import org.junit.After;

import java.io.IOException;
import java.util.List;

public class DiskBackedMapStoreTest extends BaseMapStoreTest {
    public DiskBackedMapStoreTest() throws IOException {
        super(DiskBackedMapStore.createIntegerKeyed(
            2, Val.class, "TestStore", new Val(0), DatabaseConfig.create("DiskBackedMapStoreTest")
        ));
    }

    @After
    public void closeMap() {
        ((DiskBackedMapStore) store).close();
    }
}
