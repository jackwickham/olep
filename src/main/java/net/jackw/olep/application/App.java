package net.jackw.olep.application;

import akka.actor.ActorSystem;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import net.jackw.olep.common.KafkaConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) throws IOException {
        MetricRegistry registry = new MetricRegistry();

        File resultsDir = new File(String.format("results/%d-%d/", KafkaConfig.warehouseCount(), new Date().getTime()));
        Files.createDirectory(resultsDir.toPath());

        CsvReporter reporter = CsvReporter.forRegistry(registry)
            .formatFor(Locale.UK)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(resultsDir);
        // Wait a minute for the system to warm up, then collect metrics every 30s
        reporter.start(60L, 30L, TimeUnit.SECONDS);

        ActorSystem system = ActorSystem.create("olep");

        start(registry, system);
    }

    public static void start(MetricRegistry registry, ActorSystem system) {
        for (int i = 1; i < KafkaConfig.warehouseCount(); i += 200) {
            int range = Math.min(200, KafkaConfig.warehouseCount() - 200 * i);
            system.actorOf(TerminalGroup.props(i, range, registry), "term-group-" + i);
        }
    }
}
