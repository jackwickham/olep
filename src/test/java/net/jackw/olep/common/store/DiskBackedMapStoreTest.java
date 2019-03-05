package net.jackw.olep.common.store;

import net.jackw.olep.common.DatabaseConfig;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class DiskBackedMapStoreTest extends BaseMapStoreTest {
    private DiskBackedMapStore<Val, Val> store;

    @Before
    public void initialiseStore() throws IOException {
        store = DiskBackedMapStore.create(
            2, Val.class, Val.class, "DiskBackedMapStoreTest", new Val(1), new Val(2),
            DatabaseConfig.create("DiskBackedMapStoreTest"), new ValSerde(), new ValSerde()
        );
    }

    @Override
    protected WritableKeyValueStore<Val, Val> getStore() {
        return store;
    }

    @After
    public void closeMap() {
        store.close();
    }

    public static class ValSerde implements SizedWriter<Val>, SizedReader<Val>{
        @NotNull
        @Override
        public Val read(Bytes in, long size, @Nullable Val using) {
            return new Val(in.readInt());
        }

        @Override
        public long size(@NotNull Val toWrite) {
            return 4;
        }

        @Override
        public void write(Bytes out, long size, @NotNull Val toWrite) {
            out.writeInt(toWrite.getX());
        }
    }
}
