package net.jackw.olep.common.records;

import net.openhft.chronicle.bytes.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CustomerNameKeySerdeTest {
    @Test
    public void testDeserializesToSameValue() {
        CustomerNameKey cnk = new CustomerNameKey("BARBAROUGHT", 89, 123);
        CustomerNameKeySerde serde = CustomerNameKeySerde.getInstance();
        long size = serde.size(cnk);
        Bytes bytes = Bytes.allocateDirect(size);
        serde.write(bytes, size, cnk);

        assertEquals(size, bytes.writePosition());

        assertEquals(cnk, serde.read(bytes, size, null));
    }
}
