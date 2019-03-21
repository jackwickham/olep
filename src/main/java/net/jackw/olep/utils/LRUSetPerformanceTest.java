package net.jackw.olep.utils;

import net.jackw.olep.common.LRUSet;
import net.jackw.olep.common.LockingLRUSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class LRUSetPerformanceTest {
    private final Set<Integer> set;
    private final int maxValue;
    private final AtomicBoolean stop = new AtomicBoolean(false);

    public LRUSetPerformanceTest(Set<Integer> set, int maxValue) {
        this.set = set;
        this.maxValue = maxValue;
    }

    public int readTest(int items) {
        boolean runUntilStopped = (items == 0);
        if (runUntilStopped) {
            // Run in 1000 item intervals
            items = 1000;
        }

        int value = 0;
        int contained = 0;
        do {
            for (int i = 0; i < items; i++) {
                boolean contains = set.contains(value);
                if (contains) {
                    ++contained;
                }
                if (++value > maxValue) {
                    value = 0;
                }
            }
            items = 1000;
        } while (runUntilStopped && !stop.get());
        return contained;
    }

    public void writeTest(int items) {
        boolean runUntilStopped = (items == 0);
        if (runUntilStopped) {
            items = 1000;
        }

        int value = 0;
        do {
            for (int i = 0; i < items; i++) {
                set.add(value);
                if (++value > maxValue) {
                    value = 0;
                }
            }
            items = 1000;
        } while (runUntilStopped && !stop.get());
    }

    public long runTest(Runnable test, Runnable... others) throws InterruptedException {
        stop.set(false);

        Thread[] threads = new Thread[others.length];
        for (int i = 0; i < others.length; i++) {
            threads[i] = new Thread(others[i]);
            threads[i].start();
        }


        long startTime = System.nanoTime();

        test.run();

        long duration = System.nanoTime() - startTime;

        stop.set(true);

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        return duration;
    }

    public long readWithBackgroundWriteBenchmark() throws InterruptedException {
        return runTest(
            () -> readTest(100000),
            () -> writeTest(0)
        );
    }

    public long readWithBackgroundReadBenchmark() throws InterruptedException {
        return runTest(
            () -> readTest(100000),
            () -> readTest(0)
        );
    }

    public long readOnlyBenchmark() throws InterruptedException {
        writeTest(100);
        return runTest(
            () -> readTest(100000)
        );
    }

    public long writeWithBackgroundReadBenchmark() throws InterruptedException {
        return runTest(
            () -> writeTest(100000),
            () -> readTest(0)
        );
    }

    public long writeOnlyBenchmark() throws InterruptedException {
        return runTest(
            () -> writeTest(100000)
        );
    }

    public long writeWithBackgroundWriteBenchmark() throws InterruptedException {
        return runTest(
            () -> writeTest(100000),
            () -> writeTest(0)
        );
    }

    /**
     * Run the benchmark, with 100 warmup runs (to give the JVM chance to optimise) and 100 measured runs
     *
     * @param perf The benchmark function, which returns the duration
     * @return A Results object containing the benchmark results
     */
    public static Results test(InterruptibleLongSupplier perf) throws InterruptedException {
        Results results = new Results();

        for (int i = 0; i < 100; i++) {
            perf.get();
        }
        for (int i = 0; i < 100; i++) {
            results.add(perf.get());
        }
        return results;
    }

    public static void main(String[] args) throws InterruptedException {
        LockingLRUSet<Integer> lockingSet = new LockingLRUSet<>(100);
        LRUSetPerformanceTest lockingPerf = new LRUSetPerformanceTest(lockingSet, 500);

        LRUSet<Integer> lockFreeLRUSet = new LRUSet<>(100);
        LRUSetPerformanceTest lockFreePerf = new LRUSetPerformanceTest(lockFreeLRUSet, 500);

        Results readOnlyLockingResults = test(lockingPerf::readOnlyBenchmark);
        Results readOnlyLockFreeResults = test(lockFreePerf::readOnlyBenchmark);

        System.out.printf("Read only benchmark:\n    locking: %s\n  lock free: %s\n", readOnlyLockingResults, readOnlyLockFreeResults);

        Results writeOnlyLockingResults = test(lockingPerf::writeOnlyBenchmark);
        Results writeOnlyLockFreeResults = test(lockFreePerf::writeOnlyBenchmark);

        System.out.printf("Write only benchmark:\n    locking: %s\n  lock free: %s\n", writeOnlyLockingResults, writeOnlyLockFreeResults);

        Results readReadLockingResults = test(lockingPerf::readWithBackgroundReadBenchmark);
        Results readReadLockFreeResults = test(lockFreePerf::readWithBackgroundReadBenchmark);

        System.out.printf("Read with bg read benchmark:\n    locking: %s\n  lock free: %s\n", readReadLockingResults, readReadLockFreeResults);

        Results readWriteLockingResults = test(lockingPerf::readWithBackgroundWriteBenchmark);
        Results readWriteLockFreeResults = test(lockFreePerf::readWithBackgroundWriteBenchmark);

        System.out.printf("Read with bg write benchmark:\n    locking: %s\n  lock free: %s\n", readWriteLockingResults, readWriteLockFreeResults);

        Results writeReadLockingResults = test(lockingPerf::writeWithBackgroundReadBenchmark);
        Results writeReadLockFreeResults = test(lockFreePerf::writeWithBackgroundReadBenchmark);

        System.out.printf("Write with bg read benchmark:\n    locking: %s\n  lock free: %s\n", writeReadLockingResults, writeReadLockFreeResults);

        Results writeWriteLockingResults = test(lockingPerf::writeWithBackgroundWriteBenchmark);
        Results writeWriteLockFreeResults = test(lockFreePerf::writeWithBackgroundWriteBenchmark);

        System.out.printf("Write with bg write benchmark:\n    locking: %s\n  lock free: %s\n", writeWriteLockingResults, writeWriteLockFreeResults);

        System.out.println("x = [\"Read only\", \"Write only\", \"Read with background read\", \"Read with background write\", \"Write with background read\", \"Write with background write\"]");
        System.out.printf("locking = [%d, %d, %d, %d, %d, %d]\n",
            readOnlyLockingResults.getMean(), writeOnlyLockingResults.getMean(),
            readReadLockingResults.getMean(), readWriteLockingResults.getMean(),
            writeReadLockingResults.getMean(), writeWriteLockingResults.getMean()
        );
        System.out.printf("locking_stddev = [%d, %d, %d, %d, %d, %d]\n",
            readOnlyLockingResults.getStddev(), writeOnlyLockingResults.getStddev(),
            readReadLockingResults.getStddev(), readWriteLockingResults.getStddev(),
            writeReadLockingResults.getStddev(), writeWriteLockingResults.getStddev()
        );
        System.out.printf("lock_free = [%d, %d, %d, %d, %d, %d]\n",
            readOnlyLockFreeResults.getMean(), writeOnlyLockFreeResults.getMean(),
            readReadLockFreeResults.getMean(), readWriteLockFreeResults.getMean(),
            writeReadLockFreeResults.getMean(), writeWriteLockFreeResults.getMean()
        );
        System.out.printf("lock_free_stddev = [%d, %d, %d, %d, %d, %d]\n",
            readOnlyLockFreeResults.getStddev(), writeOnlyLockFreeResults.getStddev(),
            readReadLockFreeResults.getStddev(), readWriteLockFreeResults.getStddev(),
            writeReadLockFreeResults.getStddev(), writeWriteLockFreeResults.getStddev()
        );
    }

    public static class Results {
        private final List<Long> results;

        public Results() {
            this.results = new ArrayList<>();
        }

        public void add(long time) {
            results.add(time);
        }

        public long getMean() {
            long total = 0;
            for (long time : results) {
                total += time;
            }
            return total / results.size();
        }

        public long getStddev() {
            long squareTotal = 0;
            long total = 0;
            for (long time : results) {
                squareTotal += time * time;
                total += time;
            }
            long mean = total / results.size();
            return (long) Math.sqrt((double) squareTotal / results.size() - mean * mean);
        }

        @Override
        public String toString() {
            return String.format("%d ±%d", getMean(), getStddev());
        }
    }

    @FunctionalInterface
    public interface InterruptibleLongSupplier {
        long get() throws InterruptedException;
    }
}