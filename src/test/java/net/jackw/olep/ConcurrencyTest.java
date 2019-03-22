package net.jackw.olep;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

public class ConcurrencyTest {
    protected void concurrentTest(Runnable setup, IntConsumer test, Runnable teardown, int numThreads, int numRuns) throws Throwable {
        AtomicReference<Throwable> error = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(numThreads);

        for (int run = 0; run < numRuns; run++) {
            final Thread[] threads = new Thread[numThreads];
            setup.run();

            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        barrier.await();
                        test.accept(threadId);
                    } catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[i].setUncaughtExceptionHandler((t, e) -> error.set(e));
                threads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            teardown.run();

            if (error.get() != null) {
                throw error.get();
            }
        }
    }
}
