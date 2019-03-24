package net.jackw.olep.common;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class InterThreadWorkQueueTest {
    @Test(timeout = 100)
    public void testRequest() throws InterruptedException {
        final InterThreadWorkQueue q = new InterThreadWorkQueue(1);
        new Thread(() -> {
            try {
                Thread.sleep(20);
                q.execute();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        assertEquals(5, (int) q.request(() -> 5));
    }

    @Test(timeout = 100)
    public void testExecuteRunsAllTasks() throws InterruptedException {
        final InterThreadWorkQueue q = new InterThreadWorkQueue(1);
        final AtomicInteger sum = new AtomicInteger();

        Thread t1 = new Thread(() -> {
            try {
                sum.addAndGet(q.request(() -> 3));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        t1.start();
        Thread t2 = new Thread(() -> {
            try {
                sum.addAndGet(q.request(() -> 5));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        t2.start();

        Thread.sleep(20);

        q.execute();

        t1.join();
        t2.join();

        assertEquals(8, sum.get());
    }
}
