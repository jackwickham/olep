package net.jackw.olep.common;

import net.jackw.olep.ConcurrencyTest;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class LockFreeBatchingLRUSetConcurrencyTest extends ConcurrencyTest {
    private LockFreeBatchingLRUSet<Object> set;
    private static final int NUM_RUNS = 5000;

    @Test
    public void testConcurrentAddsOfSameElementOnlySucceedOnce() throws Throwable {
        final AtomicInteger successfulInsertionCount = new AtomicInteger();
        final Object o1 = new Object();

        concurrentTest(
            () -> {
                successfulInsertionCount.set(0);
                set = new LockFreeBatchingLRUSet<>(3);
            },
            _i -> {
                if (set.add(o1)) {
                    successfulInsertionCount.incrementAndGet();
                }
            },
            () -> {
                assertEquals(1, successfulInsertionCount.get());
                assertTrue(set.contains(o1));
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
                set = new LockFreeBatchingLRUSet<>(4);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                for (int i = 0; i < os.length; i++) {
                    set.contains(os[i]);
                }
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
                set = new LockFreeBatchingLRUSet<>(2);
                set.add(initialElements[0]);
                set.add(initialElements[1]);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                for (int i = 0; i < os.length; i++) {
                    set.contains(os[i]);
                }
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
                set = new LockFreeBatchingLRUSet<>(1);
            },
            i -> {
                assertTrue(set.add(os[i]));
            },
            () -> {
                for (int i = 0; i < os.length; i++) {
                    set.contains(os[i]);
                }
            },
            4,
            NUM_RUNS
        );
    }
}
