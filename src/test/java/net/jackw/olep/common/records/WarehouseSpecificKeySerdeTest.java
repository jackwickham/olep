package net.jackw.olep.common.records;

import net.openhft.chronicle.bytes.Bytes;
import org.junit.Test;

import static org.junit.Assert.*;

public class WarehouseSpecificKeySerdeTest {
    @Test
    public void testDeserializesToSameValue() {
        WarehouseSpecificKey wsk = new WarehouseSpecificKey(10, 123);
        WarehouseSpecificKeySerde serde = WarehouseSpecificKeySerde.getInstance();
        long size = serde.size(wsk);
        Bytes bytes = Bytes.allocateDirect(size);
        serde.write(bytes, size, wsk);

        assertEquals(size, bytes.writePosition());

        assertEquals(wsk, serde.read(bytes, size, null));
    }

    @Test
    public void testKafkaDeserializesToSameValue() {
        WarehouseSpecificKey wsk = new WarehouseSpecificKey(10, 123);
        WarehouseSpecificKeySerde serde = WarehouseSpecificKeySerde.getInstance();
        byte[] bytes = serde.serialize("test", wsk);

        assertEquals(wsk, serde.deserialize("test", bytes));
    }
}
