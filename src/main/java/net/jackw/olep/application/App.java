package net.jackw.olep.application;

import akka.actor.ActorSystem;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import net.jackw.olep.common.KafkaConfig;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) {
        MetricRegistry registry = new MetricRegistry();

        CsvReporter reporter = CsvReporter.forRegistry(registry)
            .formatFor(Locale.UK)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(new File("results/"));
        // Wait a minute for the system to warm up, then collect metrics every 30s
        reporter.start(60L, 30L, TimeUnit.SECONDS);

        ActorSystem system = ActorSystem.create("olep");

        start(registry, system);
    }

    public static void start(MetricRegistry registry, ActorSystem system) {
        for (int i = 1; i < KafkaConfig.warehouseCount(); i += 10) {
            system.actorOf(TerminalGroup.props(i, 10, registry), "term-group-" + i);
        }
    }
}
