package net.jackw.olep.application;

import akka.actor.ActorSystem;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import net.jackw.olep.common.KafkaConfig;

public class App {
    public static void main(String[] args) {
        MetricRegistry registry = new MetricRegistry();
        final JmxReporter reporter = JmxReporter.forRegistry(registry).build();
        reporter.start();

        ActorSystem system = ActorSystem.create("olep");
        for (int i = 1; i < KafkaConfig.warehouseCount(); i += 10) {
            system.actorOf(TerminalGroup.props(i, 10, registry), "term-group-" + i);
        }
    }
}
