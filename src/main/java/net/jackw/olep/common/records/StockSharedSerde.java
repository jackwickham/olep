package net.jackw.olep.common.records;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class StockSharedSerde implements SizedReader<StockShared>, SizedWriter<StockShared>, ReadResolvable<StockSharedSerde> {
    private static final StockSharedSerde INSTANCE = new StockSharedSerde();

    public StockSharedSerde() { }

    @NotNull
    @Override
    public StockShared read(Bytes in, long size, @Nullable StockShared using) {
        return new StockShared(
            in.readInt(), in.readInt(), in.readUtf8(), in.readUtf8(), in.readUtf8(), in.readUtf8(), in.readUtf8(),
            in.readUtf8(), in.readUtf8(), in.readUtf8(), in.readUtf8(), in.readUtf8(), in.readUtf8()
        );
    }

    @Override
    public long size(@NotNull StockShared toWrite) {
        return toWrite.data.length() + 259L;
    }

    @Override
    public void write(Bytes out, long size, @NotNull StockShared toWrite) {
        out.writeInt(toWrite.itemId)
            .writeInt(toWrite.warehouseId)
            .writeUtf8(toWrite.dist01)
            .writeUtf8(toWrite.dist02)
            .writeUtf8(toWrite.dist03)
            .writeUtf8(toWrite.dist04)
            .writeUtf8(toWrite.dist05)
            .writeUtf8(toWrite.dist06)
            .writeUtf8(toWrite.dist07)
            .writeUtf8(toWrite.dist08)
            .writeUtf8(toWrite.dist09)
            .writeUtf8(toWrite.dist10)
            .writeUtf8(toWrite.data);
    }

    @NotNull
    @Override
    public StockSharedSerde readResolve() {
        return INSTANCE;
    }

    public static StockSharedSerde getInstance() {
        return INSTANCE;
    }
}
