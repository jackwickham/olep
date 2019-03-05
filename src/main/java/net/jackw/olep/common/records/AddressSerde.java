package net.jackw.olep.common.records;

import net.openhft.chronicle.bytes.Bytes;

import javax.annotation.Nonnull;

public class AddressSerde {
    private static final AddressSerde INSTANCE = new AddressSerde();

    public AddressSerde() { }

    public Address read(Bytes in) {
        return new Address(in.readUtf8(), in.readUtf8(), in.readUtf8(), in.readUtf8(), in.readUtf8());
    }

    @SuppressWarnings("IntLongMath")
    public long size(@Nonnull Address toWrite) {
        return toWrite.street1.length() + toWrite.street2.length() + toWrite.city.length() +
            toWrite.state.length() + toWrite.zip.length();
    }

    public void write(Bytes out, @Nonnull Address toWrite) {
        out.writeUtf8(toWrite.street1)
            .writeUtf8(toWrite.street2)
            .writeUtf8(toWrite.city)
            .writeUtf8(toWrite.state)
            .writeUtf8(toWrite.zip);
    }

    public static AddressSerde getInstance() {
        return INSTANCE;
    }
}
