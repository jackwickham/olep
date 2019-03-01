package net.jackw.olep.integration;

import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.view.ViewReadAdapter;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class LogViewAdapterStub implements ViewReadAdapter, Serializable, AutoCloseable {
    private Registry registry;
    private static final String name = "view/0";

    public LogViewAdapterStub(Registry registry) {
        this.registry = registry;
    }

    public static LogViewAdapterStub create(String registryServer) {
        try {
            Registry registry = LocateRegistry.getRegistry(registryServer);
            LogViewAdapterStub adapter = new LogViewAdapterStub(registry);

            registry.rebind(name, adapter);

            return adapter;
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            registry.unbind(name);
        } catch (NotBoundException e) {
            // pass
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OrderStatusResult orderStatus(int customerId, int districtId, int warehouseId) {
        throw new RuntimeException("Can't perform transactions on log view adapter stub");
    }

    @Override
    public OrderStatusResult orderStatus(String customerLastName, int districtId, int warehouseId) {
        throw new RuntimeException("Can't perform transactions on log view adapter stub");
    }

    @Override
    public int stockLevel(int districtId, int warehouseId, int stockThreshold) {
        throw new RuntimeException("Can't perform transactions on log view adapter stub");
    }
}
