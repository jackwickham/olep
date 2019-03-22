package net.jackw.olep.common;

import net.jackw.olep.ConcurrencyTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class LockFreeLRUSetConcurrencyTest extends ConcurrencyTest {
    private LockFreeLRUSet<Object> set;
    private static final int NUM_RUNS = 5000;

    @Test
    public void testConcurrentAddsOfSameElementOnlySucceedOnce() throws Throwable {
        final AtomicInteger successfulInsertionCount = new AtomicInteger();
        final Object o1 = new Object();

        concurrentTest(
            () -> {
                successfulInsertionCount.set(0);
                set = new LockFreeLRUSet<>(3);
            },
            _i -> {
                if (set.add(o1)) {
                    successfulInsertionCount.incrementAndGet();
                }
            },
            () -> {
                assertEquals(1, successfulInsertionCount.get());
                assertEquals(1, set.size());
                assertThat(set, Matchers.contains(o1));
            },
            4,
            NUM_RUNS
        );
    }

    @Test
    public void testMultipleConcurrentInsertsOfDifferentElementsAllSucceed() throws Throwable {
        final Object[] os = {new Object(), new Object(), new Object(), new Object()};

        concurrentTest(
            () -> {
                set = new LockFreeLRUSet<>(4);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                assertEquals(4, set.size());
                assertThat(set, Matchers.containsInAnyOrder(os));
            },
            4,
            NUM_RUNS
        );
    }

    @Test
    public void testConcurrentInsertionsAndAutomaticDeletionsPreserveInvariants() throws Throwable {
        final Object[] os = {new Object(), new Object()};
        final Object[] initialElements = {new Object(), new Object()};

        concurrentTest(
            () -> {
                set = new LockFreeLRUSet<>(2);
                Collections.addAll(set, initialElements);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                assertThat(set, Matchers.containsInAnyOrder(os));
            },
            2,
            NUM_RUNS
        );
    }

    @Test
    public void testElementInsertedAndDeleted() throws Throwable {
        final Object[] os = {new Object(), new Object(), new Object(), new Object()};

        concurrentTest(
            () -> {
                set = new LockFreeLRUSet<>(1);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                assertEquals(1, set.size());
                assertThat(set, Matchers.contains(Matchers.in(os)));
            },
            4,
            NUM_RUNS
        );
    }
}
