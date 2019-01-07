package net.jackw.olep.view;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class StandaloneRegistry {
    public static void start() {
        try {
            LocateRegistry.createRegistry(1099);
        } catch (RemoteException e) {
            // Means a registry is already listening, so there's nothing more to do
        }
    }

    public static void main(String[] args) throws InterruptedException {
        start();
        // Block the current thread until it gets killed
        Thread.currentThread().join();
    }
}
