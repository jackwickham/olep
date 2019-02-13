package net.jackw.olep.metrics;

public class Timer {
    private long startTime;

    private Timer() {
        startTime = System.nanoTime();
    }

    public static Timer start() {
        return new Timer();
    }

    public long getElapsed() {
        return System.nanoTime() - startTime;
    }
}
