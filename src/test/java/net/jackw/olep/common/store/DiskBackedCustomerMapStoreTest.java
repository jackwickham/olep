package net.jackw.olep.common.store;

import net.jackw.olep.common.DatabaseConfig;
import org.junit.After;

import java.io.IOException;
import java.util.List;

public class DiskBackedCustomerMapStoreTest extends BaseCustomerStoreTest {
    public DiskBackedCustomerMapStoreTest() throws IOException {
        super(new DiskBackedCustomerMapStore(DatabaseConfig.create(List.of())));
    }

    @After
    public void closeMap() {
        ((DiskBackedCustomerMapStore) this.store).close();
    }
}
