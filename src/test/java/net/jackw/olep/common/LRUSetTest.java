package net.jackw.olep.common;

import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

@SuppressWarnings("ModifiedButNotUsed")
public class LRUSetTest {
    @Test(expected = IllegalArgumentException.class)
    public void testZeroCapacityThrowsException() {
        new LRUSet<>(0);
    }

    @Test
    public void testInitiallyHasZeroSize() {
        LRUSet<Object> set = new LRUSet<>(3);
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testAddIncreasesSizeAndContains() {
        LRUSet<Object> set = new LRUSet<>(3);
        Object o = new Object();
        set.add(o);

        assertEquals(1, set.size());
        assertTrue(set.contains(o));
        assertFalse(set.isEmpty());
    }

    @Test
    public void testInsertReturnsTrue() {
        LRUSet<Object> set = new LRUSet<>(3);
        Object o = new Object();

        assertTrue(set.add(o));
    }

    @Test
    public void testInsertingOverCapacityInsertsNewObject() {
        LRUSet<Object> set = new LRUSet<>(3);
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        Object o4 = new Object();

        set.add(o1);
        set.add(o2);
        set.add(o3);

        assertTrue(set.add(o4));

        assertTrue(set.contains(o4));
    }

    @Test
    public void testInsertingOverCapacityRemovesEldestOnly() {
        LRUSet<Object> set = new LRUSet<>(3);
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        Object o4 = new Object();

        set.add(o1);
        set.add(o2);
        set.add(o3);
        set.add(o4);

        assertFalse(set.contains(o1));
        assertTrue(set.contains(o2));
        assertTrue(set.contains(o3));
        assertEquals(3, set.size());
    }

    @Test
    public void testInsertingOverCapacityAgainRemovesCorrectNextElement() {
        LRUSet<Object> set = new LRUSet<>(3);
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        Object o4 = new Object();
        Object o5 = new Object();

        set.add(o1);
        set.add(o2);
        set.add(o3);
        set.add(o4);
        set.add(o5);

        assertFalse(set.contains(o1));
        assertFalse(set.contains(o2));
        assertTrue(set.contains(o3));
        assertEquals(3, set.size());
    }

    @Test
    public void testIteratorContainsOnlyPresentElements() {
        LRUSet<Object> set = new LRUSet<>(3);
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        Object o4 = new Object();

        set.add(o1);
        set.add(o2);
        set.add(o3);
        set.add(o4);

        List<Object> iteratorElements = Lists.newArrayList(set.iterator());

        assertEquals(3, iteratorElements.size());
        assertThat(iteratorElements, Matchers.containsInAnyOrder(o2, o3, o4));
    }

    @Test
    public void testDuplicateInsertionsDontIncreaseSize() {
        LRUSet<Object> set = new LRUSet<>(3);
        Object o1 = new Object();

        set.add(o1);
        set.add(o1);

        assertEquals(1, set.size());
    }

    @Test
    public void testDuplicateInsertionsHaveNoSideEffects() {
        LRUSet<Object> set = new LRUSet<>(3);
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();

        set.add(o1);
        set.add(o2);
        set.add(o3);

        assertFalse(set.add(o3));

        assertTrue(set.contains(o1));
        assertTrue(set.contains(o3));
    }

    @Test(expected = NullPointerException.class)
    public void testCantInsertNull() {
        LRUSet<Object> set = new LRUSet<>(3);
        set.add(null);
    }
}
