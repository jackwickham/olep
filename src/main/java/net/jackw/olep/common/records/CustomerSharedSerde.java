package net.jackw.olep.common.records;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CustomerSharedSerde implements BytesReader<CustomerShared>, BytesWriter<CustomerShared>, ReadResolvable<CustomerSharedSerde> {
    private static final CustomerSharedSerde INSTANCE = new CustomerSharedSerde();

    public CustomerSharedSerde() { }

    @NotNull
    @Override
    public CustomerShared read(Bytes in, @Nullable CustomerShared using) {
        return new CustomerShared(
            in.readInt(), in.readInt(), in.readInt(), in.readUtf8(), in.readUtf8(), in.readUtf8(),
            AddressSerde.getInstance().read(in), in.readUtf8(), in.readLong(), Credit.fromByteValue(in.readByte()),
            in.readBigDecimal(), in.readBigDecimal()
        );
    }

    @Override
    public void write(Bytes out, @NotNull CustomerShared toWrite) {
        out.writeInt(toWrite.id)
            .writeInt(toWrite.districtId)
            .writeInt(toWrite.warehouseId)
            .writeUtf8(toWrite.firstName)
            .writeUtf8(toWrite.middleName)
            .writeUtf8(toWrite.lastName);
        AddressSerde.getInstance().write(out, toWrite.address);
        out.writeUtf8(toWrite.phone)
            .writeLong(toWrite.since)
            .writeByte(toWrite.credit.getByteValue())
            .writeBigDecimal(toWrite.creditLimit);
        out.writeBigDecimal(toWrite.discount);
    }

    @NotNull
    @Override
    public CustomerSharedSerde readResolve() {
        return INSTANCE;
    }

    public static CustomerSharedSerde getInstance() {
        return INSTANCE;
    }
}
