package net.jackw.olep.view;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import net.jackw.olep.common.Arguments;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.StreamsApp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ViewApp implements AutoCloseable {
    private List<LogViewAdapter> adapters;

    public ViewApp(DatabaseConfig config) throws RemoteException, InterruptedException {
        adapters = new ArrayList<>(config.getViewThreads());

        for (int i = 0; i < config.getViewThreads(); i++) {
            adapters.add(LogViewAdapter.init(config));
        }
    }

    public void start() {
        for (LogViewAdapter adapter : adapters) {
            adapter.start();
        }
    }

    public ListenableFuture<List<Void>> getReadyFuture() {
        return Futures.allAsList(adapters.stream().map(LogViewAdapter::getReadyFuture).collect(Collectors.toList()));
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments(args);

        ViewApp app = new ViewApp(arguments.getConfig());
        app.start();

        // Add a shutdown listener to gracefully handle Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread("view-adapter-shutdown-hook") {
            @Override
            public void run() {
                try {
                    app.close();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        });

        // Wait for the adapter to be ready
        app.getReadyFuture().get();
        log.info("View loaded");
        StreamsApp.createReadyFile(arguments.getReadyFileArg());

        for (LogViewAdapter adapter : app.adapters) {
            adapter.join();
        }

        System.exit(0);
    }

    @Override
    public void close() throws InterruptedException {
        Exception ex = null;
        for (LogViewAdapter adapter : adapters) {
            try {
                adapter.close();
            } catch (Exception e) {
                if (ex == null) {
                    ex = e;
                } else {
                    ex.addSuppressed(e);
                }
            }
        }
        if (ex != null) {
            // The only checked exception that can be thrown is InterruptedException
            if (ex instanceof InterruptedException) {
                throw (InterruptedException) ex;
            } else {
                throw (RuntimeException) ex;
            }
        }
    }

    private static Logger log = LogManager.getLogger();
}
