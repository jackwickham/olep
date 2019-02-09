package net.jackw.olep.application.transaction;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;

public class TimerSupplier implements MetricRegistry.MetricSupplier<Timer> {
    @Override
    public Timer newMetric() {
        return new Timer(new SlidingTimeWindowReservoir(30, TimeUnit.SECONDS));
    }
}
