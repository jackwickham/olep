package net.jackw.olep.utils;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.StreamsApp;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.LogViewAdapter;
import net.jackw.olep.view.StandaloneRegistry;
import net.jackw.olep.worker.WorkerApp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    public static void main(String[] args) throws InterruptedException, IOException {
        Arguments arguments = new Arguments(args);
        DatabaseConfig config = arguments.getConfig();

        StandaloneRegistry.start();

        List<ListenableFuture<?>> futures = new ArrayList<>(3);

        final VerifierApp verifierApp = new VerifierApp(config);
        final WorkerApp workerApp = new WorkerApp(config);
        LogViewAdapter logViewAdapter = LogViewAdapter.init(config.getBootstrapServers(), config.getViewRegistryHost(), config);

        // Add a shutdown listener to gracefully handle Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                workerApp.close();
                verifierApp.close();
                logViewAdapter.close();
            } catch (Exception e) {
                log.error(e);
            }
        }));

        // Create futures that will resolve when the verifier and worker apps are set up and (more or less) ready to
        // process messages
        futures.add(ListenableFutureTask.create(() -> {
            verifierApp.start();
            return null;
        }));
        futures.add(ListenableFutureTask.create(() -> {
            workerApp.start();
            return null;
        }));

        // logViewAdapter.start() does all the work in a new thread
        logViewAdapter.start();

        futures.add(logViewAdapter.getReadyFuture());

        // Then when the futures are done, mark it as ready
        Futures.allAsList(futures).addListener(() -> {
            StreamsApp.createReadyFile(arguments.getReadyFileArg());
        }, MoreExecutors.directExecutor());

        // Block until Ctrl+C
        logViewAdapter.join();
    }

    private static Logger log = LogManager.getLogger();
}
