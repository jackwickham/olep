package net.jackw.olep.acceptance;

import akka.actor.ActorSystem;
import net.jackw.olep.application.RootActor;
import net.jackw.olep.common.DatabaseConfig;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AppRunner {
    private long durationMillis;
    private DatabaseConfig config;

    public AppRunner(long durationSeconds, DatabaseConfig config) {
        this.durationMillis = durationSeconds * 1000;
        this.config = config;
    }

    @Test
    public void runApp() throws InterruptedException, TimeoutException {
        ActorSystem system = ActorSystem.create(getClass().getSimpleName());

        system.actorOf(RootActor.props(config));

        Thread.sleep(durationMillis);

        Await.ready(system.terminate(), Duration.create(20, TimeUnit.SECONDS));
    }
}
