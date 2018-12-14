package net.jackw.olep.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class WarehousePartitionerTest {
    @Test
    public void testWarehousePartitioner() {
        WarehousePartitioner warehousePartitioner = new WarehousePartitioner();
        assertEquals(2, (int) warehousePartitioner.partition(
            "topic", new TransactionWarehouseKey(1L, 20), new Object(), 6
        ));
    }
}
