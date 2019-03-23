package net.jackw.olep.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class BatchingLRUSetTest {
    @Test
    public void testAddContains() {
        BatchingLRUSet<Object> set = new BatchingLRUSet<>(3);
        Object o = new Object();
        assertTrue(set.add(o));
        assertTrue(set.contains(o));
    }

    @Test
    public void testDuplicateAddsReturnFalse() {
        BatchingLRUSet<Object> set = new BatchingLRUSet<>(3);
        Object o = new Object();
        assertTrue(set.add(o));
        assertFalse(set.add(o));
        assertTrue(set.contains(o));
    }

    @Test
    public void testContainsAndAddConsistent() {
        BatchingLRUSet<Object> set = new BatchingLRUSet<>(3);
        Object o = new Object();
        assertTrue(set.add(o));
        for (int i = 0; i < 12; i++) {
            boolean contained = set.contains(o);
            if (contained) {
                assertFalse(set.add(o));
            } else {
                assertTrue(set.add(o));
                assertTrue(set.contains(o));
                return;
            }
            set.add(new Object());
        }
    }

    @Test
    public void testMultipleAdds() {
        BatchingLRUSet<Object> set = new BatchingLRUSet<>(3);
        Object[] os = {new Object(), new Object(), new Object(), new Object(), new Object(), new Object()};
        assertTrue(set.add(os[0]));
        assertTrue(set.add(os[1]));
        assertTrue(set.add(os[2]));
        assertTrue(set.contains(os[0]));
        assertTrue(set.contains(os[1]));
        assertTrue(set.contains(os[2]));
        assertTrue(set.add(os[3]));
        assertTrue(set.add(os[4]));
        assertTrue(set.add(os[5]));
        assertTrue(set.contains(os[3]));
        assertTrue(set.contains(os[4]));
        assertTrue(set.contains(os[5]));
    }

    @Test
    public void testLRURemoved() {
        BatchingLRUSet<Object> set = new BatchingLRUSet<>(3);
        Object o = new Object();
        assertTrue(set.add(o));
        for (int i = 0; i < 12; i++) {
            set.add(new Object());
            if (set.contains(o)) {
                return;
            }
        }
        fail("First item not removed after 12 insertions");
    }
}
