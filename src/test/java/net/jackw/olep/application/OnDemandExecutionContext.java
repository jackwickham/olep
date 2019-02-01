package net.jackw.olep.application;

import scala.concurrent.ExecutionContext;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * An execution context that holds onto its tasks, then executes them synchronously when run() is called.
 */
public class OnDemandExecutionContext implements ExecutionContext {
    private Queue<Runnable> queuedTasks = new ArrayDeque<>();

    @Override
    public void execute(Runnable runnable) {
        queuedTasks.add(runnable);
    }

    @Override
    public void reportFailure(Throwable cause) {
        throw new RuntimeException(cause);
    }

    /**
     * Run all tasks that are currently in the queue, as well as all tasks that are added to the queue by those tasks
     */
    public void run() {
        Runnable task;
        while ((task = queuedTasks.poll()) != null) {
            task.run();
        }
    }
}
