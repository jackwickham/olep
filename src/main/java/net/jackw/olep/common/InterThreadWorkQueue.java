package net.jackw.olep.common;

import com.google.common.base.Preconditions;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

public class InterThreadWorkQueue {
    private final BlockingQueue<Task<?>> queue;

    public InterThreadWorkQueue(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    public <R> R request(Supplier<R> task) throws InterruptedException {
        Task<R> element = new Task<>(task);
        queue.put(element);
        return element.getResult();
    }

    @SuppressWarnings("unchecked")
    public void execute() {
        Task element;
        while ((element = queue.poll()) != null) {
            element.setResult(element.getTask().get());
        }
    }

    private static class Task<R> {
        private final Supplier<? extends R> task;
        private volatile R result = null;

        public Task(Supplier<? extends R> task) {
            this.task = task;
        }

        public Supplier<? extends R> getTask() {
            return task;
        }

        public synchronized void setResult(R result) {
            Preconditions.checkNotNull(result);
            this.result = result;
            notifyAll();
        }

        public synchronized R getResult() throws InterruptedException {
            R res;
            while ((res = this.result) == null) {
                wait();
            }
            return res;
        }
    }
}
