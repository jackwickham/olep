package net.jackw.olep.application;

import akka.actor.ActorSystem;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.KafkaConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) throws IOException {
        DatabaseConfig config = DatabaseConfig.create(args);

        MetricRegistry registry = new MetricRegistry();

        String date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());
        File resultsDir = new File(String.format("results/%s-%d/", date, config.getWarehouseCount()));
        Files.createDirectory(resultsDir.toPath());

        CsvReporter reporter = CsvReporter.forRegistry(registry)
            .formatFor(Locale.UK)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(resultsDir);
        // Wait a minute for the system to warm up, then collect metrics every 30s
        reporter.start(60L, 30L, TimeUnit.SECONDS);

        ActorSystem system = ActorSystem.create("olep");

        start(config, registry, system);
    }

    public static void start(DatabaseConfig config, MetricRegistry registry, ActorSystem system) {
        for (int i = 1; i < config.getWarehouseCount(); i += 200) {
            int range = Math.min(200, config.getWarehouseCount() - 200 * (i-1));
            system.actorOf(TerminalGroup.props(i, range, config, registry), "term-group-" + i);
        }
    }
}
