package net.jackw.olep.common.records;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CustomerNameKeySerde implements SizedReader<CustomerNameKey>, SizedWriter<CustomerNameKey>, ReadResolvable<CustomerNameKeySerde> {
    private static final CustomerNameKeySerde INSTANCE = new CustomerNameKeySerde();

    private CustomerNameKeySerde() { }

    @NotNull
    @Override
    public CustomerNameKey read(Bytes in, long size, @Nullable CustomerNameKey using) {
        // CustomerNameKey is final, so using isn't useful
        return new CustomerNameKey(in.readUtf8(), in.readInt(), in.readInt());
    }

    @Override
    public void write(Bytes out, long size, @NotNull CustomerNameKey toWrite) {
        out.writeUtf8(toWrite.lastName)
            .writeInt(toWrite.districtId)
            .writeInt(toWrite.warehouseId);
    }

    @Override
    public long size(@NotNull CustomerNameKey toWrite) {
        // 4B for district, 4B for warehouse, length + 1B for name
        return toWrite.lastName.length() + 9L;
    }

    @NotNull
    @Override
    public CustomerNameKeySerde readResolve() {
        return INSTANCE;
    }

    public static CustomerNameKeySerde getInstance() {
        return INSTANCE;
    }
}
