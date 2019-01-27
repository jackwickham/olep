package net.jackw.olep.view;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.store.SharedCustomerStore;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class InMemoryRMIWrapper implements AutoCloseable {
    private InMemoryAdapter adapter;
    private Registry registry;
    private final String name = "view/TODO_PARTITION_NUMBER";
    private boolean closed = false;

    @MustBeClosed
    public InMemoryRMIWrapper(String registryServer, SharedCustomerStore customerStore) throws RemoteException, AlreadyBoundException {
        super();

        adapter = new InMemoryAdapter(customerStore);

        registry = LocateRegistry.getRegistry(registryServer);
        registry.bind(name, adapter);
    }

    @Override
    public void close() throws RemoteException, NotBoundException {
        if (!closed) {
            registry.unbind(name);
            adapter.close();
            closed = true;
        }
    }

    public InMemoryAdapter getAdapter() {
        return adapter;
    }
}
