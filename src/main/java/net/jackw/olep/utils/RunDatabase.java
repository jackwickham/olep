package net.jackw.olep.utils;

import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.LogViewAdapter;
import net.jackw.olep.view.StandaloneRegistry;
import net.jackw.olep.worker.WorkerApp;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * Run all components of the database (verifier, worker, view and registry)
 */
public class RunDatabase {
    public static void main(String[] args) throws RemoteException, AlreadyBoundException, NotBoundException, InterruptedException, IOException {
        DatabaseConfig config = DatabaseConfig.create(args);

        StandaloneRegistry.start();

        new Thread(() -> VerifierApp.run(config), "verifier-main").start();
        new Thread(() -> WorkerApp.run(config), "worker-main").start();

        try (LogViewAdapter logViewAdapter = LogViewAdapter.init(config.getBootstrapServers(), config.getViewRegistryHost(), config)) {
            logViewAdapter.start();
            // Block until Ctrl+C
            logViewAdapter.join();
        }
    }
}
