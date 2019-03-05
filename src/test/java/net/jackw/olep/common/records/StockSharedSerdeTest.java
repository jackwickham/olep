package net.jackw.olep.common.records;

import net.jackw.olep.utils.populate.PredictableStockFactory;
import net.openhft.chronicle.bytes.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StockSharedSerdeTest {
    @Test
    public void testDeserializesToSameValue() {
        StockShared stock = PredictableStockFactory.instanceFor(10).getStockShared(5);
        StockSharedSerde serde = StockSharedSerde.getInstance();
        long size = serde.size(stock);
        Bytes bytes = Bytes.allocateDirect(size);
        serde.write(bytes, size, stock);

        assertEquals(size, bytes.writePosition());

        StockShared result = serde.read(bytes, size, null);

        // Checks the key only
        assertEquals(stock, result);

        // Check the fields individually
        assertEquals(stock.itemId, result.itemId);
        assertEquals(stock.warehouseId, result.warehouseId);
        assertEquals(stock.dist01, result.dist01);
        assertEquals(stock.dist02, result.dist02);
        assertEquals(stock.dist03, result.dist03);
        assertEquals(stock.dist04, result.dist04);
        assertEquals(stock.dist05, result.dist05);
        assertEquals(stock.dist06, result.dist06);
        assertEquals(stock.dist07, result.dist07);
        assertEquals(stock.dist08, result.dist08);
        assertEquals(stock.dist09, result.dist09);
        assertEquals(stock.dist10, result.dist10);
        assertEquals(stock.data, result.data);
    }
}
