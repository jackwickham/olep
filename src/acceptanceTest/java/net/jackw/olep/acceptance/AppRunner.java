package net.jackw.olep.acceptance;

import akka.actor.ActorSystem;
import net.jackw.olep.application.RootActor;
import net.jackw.olep.common.DatabaseConfig;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AppRunner {
    private long durationMillis;
    private DatabaseConfig config;

    public AppRunner(long durationSeconds, DatabaseConfig config) {
        this.durationMillis = durationSeconds * 1000;
        this.config = config;
    }

    @Test
    public void runApp() throws Throwable {
        AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread testThread = Thread.currentThread();

        ActorSystem system = ActorSystem.create(getClass().getSimpleName());
        system.actorOf(RootActor.props(config, e -> {
            error.set(e);
            testThread.interrupt();
        }));

        try {
            Thread.sleep(durationMillis);
        } catch (InterruptedException e) {
            // continue
        }

        Await.ready(system.terminate(), Duration.create(20, TimeUnit.SECONDS));

        if (error.get() != null) {
            throw error.get();
        }
    }
}
