package net.jackw.olep.utils;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.LogViewAdapter;
import net.jackw.olep.view.StandaloneRegistry;
import net.jackw.olep.worker.WorkerApp;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Run all components of the database (verifier, worker, view and registry)
 */
public class RunDatabase {
    public static void main(String[] args) throws RemoteException, AlreadyBoundException, NotBoundException, InterruptedException, IOException {
        Arguments arguments = new Arguments(args);
        DatabaseConfig config = arguments.getConfig();

        StandaloneRegistry.start();

        List<ListenableFuture<?>> futures = Collections.synchronizedList(new ArrayList<>(3));
        CountDownLatch threadStartLatch = new CountDownLatch(2);

        new Thread(() -> {
            VerifierApp app = new VerifierApp(config);
            futures.add(app.getBeforeStartFuture());
            threadStartLatch.countDown();
            app.run();
        }, "verifier-main").start();
        new Thread(() -> {
            WorkerApp app = new WorkerApp(config);
            futures.add(app.getBeforeStartFuture());
            threadStartLatch.countDown();
            app.run();
        }, "worker-main").start();

        try (LogViewAdapter logViewAdapter = LogViewAdapter.init(config.getBootstrapServers(), config.getViewRegistryHost(), config)) {
            logViewAdapter.start();

            futures.add(logViewAdapter.getReadyFuture());
            // Wait for all the threads to have started and registered their futures
            threadStartLatch.await();

            // Then when the futures are done, mark it as ready
            Futures.allAsList(futures).addListener(() -> {
                StreamsApp.createReadyFile(arguments.getReadyFileArg());
            }, MoreExecutors.directExecutor());

            // Block until Ctrl+C
            logViewAdapter.join();
        }
    }
}
