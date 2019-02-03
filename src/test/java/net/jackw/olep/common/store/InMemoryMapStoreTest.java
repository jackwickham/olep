package net.jackw.olep.common.store;

public class InMemoryMapStoreTest extends BaseMapStoreTest {
    public InMemoryMapStoreTest() {
        super(new InMemoryMapStore<>(2));
    }
}
