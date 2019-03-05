package net.jackw.olep.common.store;

import org.junit.Before;

public class InMemoryMapStoreTest extends BaseMapStoreTest {
    private InMemoryMapStore<Val, Val> store;

    @Before
    public void initialiseStore() {
        store = new InMemoryMapStore<>(2);
    }

    @Override
    protected WritableKeyValueStore<Val, Val> getStore() {
        return store;
    }
}
