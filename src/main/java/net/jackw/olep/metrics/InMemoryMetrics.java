package net.jackw.olep.metrics;

import java.util.ArrayList;

public class InMemoryMetrics extends Metrics {
    private ArrayList<DurationMetric> durationMetrics = new ArrayList<>();
    private ArrayList<EventMetric> eventMetrics = new ArrayList<>();

    @Override
    public synchronized void recordDuration(DurationType type, Timer timer) {
        durationMetrics.add(new DurationMetric(type, timer.getElapsed()));
    }

    @Override
    public synchronized void recordEvent(EventType type) {
        eventMetrics.add(new EventMetric(type));
    }

    public ArrayList<DurationMetric> getDurationMetrics() {
        return durationMetrics;
    }

    public ArrayList<EventMetric> getEventMetrics() {
        return eventMetrics;
    }

    public static class DurationMetric {
        public final DurationType type;
        public final long time;

        public DurationMetric(DurationType type, long time) {
            this.type = type;
            this.time = time;
        }
    }

    public static class EventMetric {
        public final EventType type;

        public EventMetric(EventType type) {
            this.type = type;
        }
    }
}
