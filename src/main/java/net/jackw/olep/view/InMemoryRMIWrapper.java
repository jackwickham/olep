package net.jackw.olep.view;

import com.google.errorprone.annotations.MustBeClosed;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class InMemoryRMIWrapper implements AutoCloseable {
    private InMemoryAdapter adapter;
    private Registry registry;
    private final String name = "view/TODO_PARTITION_NUMBER";

    @MustBeClosed
    public InMemoryRMIWrapper(String registryServer) throws RemoteException, AlreadyBoundException {
        super();

        adapter = new InMemoryAdapter();

        registry = LocateRegistry.getRegistry(registryServer);
        registry.bind(name, adapter);
    }

    @Override
    public void close() throws RemoteException, NotBoundException {
        registry.unbind(name);
        adapter.close();
    }

    public InMemoryAdapter getAdapter() {
        return adapter;
    }
}
