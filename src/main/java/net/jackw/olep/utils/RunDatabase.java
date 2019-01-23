package net.jackw.olep.utils;

import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.LogViewAdapter;
import net.jackw.olep.view.StandaloneRegistry;
import net.jackw.olep.worker.WorkerApp;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * Run all components of the database (verifier, worker, view and registry)
 */
public class RunDatabase {
    public static void main(String[] args) throws RemoteException, AlreadyBoundException, NotBoundException, InterruptedException {
        StandaloneRegistry.start();

        new Thread(() -> VerifierApp.main(new String[]{}), "verifier-main").start();
        new Thread(() -> WorkerApp.main(new String[]{}), "worker-main").start();

        try (LogViewAdapter logViewAdapter = LogViewAdapter.init("127.0.0.1:9092", "127.0.0.1")) {
            logViewAdapter.start();
            // Block until Ctrl+C
            logViewAdapter.join();
        }
    }
}
