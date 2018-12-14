package net.jackw.olep.common;

import net.jackw.olep.message.transaction_result.TransactionResultKey;
import org.junit.Test;

import static org.junit.Assert.*;

public class TransactionResultPartitionerTest {
    @Test
    public void testPartitioningPositiveIntegers() {
        assertEquals(2, TransactionResultPartitioner.partition(8, 3));
    }

    @Test
    public void testPartitioningNegativeIntegers() {
        assertEquals(2, TransactionResultPartitioner.partition(-7, 3));
    }

    @Test
    public void testConnectionIdFromKeyUsedOnInstanceMethod() {
        TransactionResultPartitioner partitioner = new TransactionResultPartitioner();
        assertEquals(1, (int) partitioner.partition("TestTopic", new TransactionResultKey(13, false), new Object(), 3));
    }
}
