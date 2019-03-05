package net.jackw.olep.common.records;

import net.openhft.chronicle.bytes.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DistrictSpecificKeySerdeTest {
    @Test
    public void testDeserializesToSameValue() {
        DistrictSpecificKey dsk = new DistrictSpecificKey(10, 89, 123);
        DistrictSpecificKeySerde serde = DistrictSpecificKeySerde.getInstance();
        long size = serde.size(dsk);
        Bytes bytes = Bytes.allocateDirect(size);
        serde.write(bytes, size, dsk);

        assertEquals(size, bytes.writePosition());

        assertEquals(dsk, serde.read(bytes, size, null));
    }

    @Test
    public void testKafkaDeserializesToSameValue() {
        DistrictSpecificKey dsk = new DistrictSpecificKey(10, 89, 123);
        DistrictSpecificKeySerde serde = DistrictSpecificKeySerde.getInstance();
        byte[] bytes = serde.serialize("test", dsk);

        assertEquals(dsk, serde.deserialize("test", bytes));
    }
}
