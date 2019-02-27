package net.jackw.olep.acceptance;

import net.jackw.olep.common.Database;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.verifier.VerifierApp;
import net.jackw.olep.view.LogViewAdapter;
import net.jackw.olep.worker.WorkerApp;

/**
 * A store for the current state, from BaseAcceptanceTest, to work around not being able to pass state from the suite
 * to the test classes
 */
public class CurrentTestState {
    private static CurrentTestState instance;

    public static CurrentTestState getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Current test state hasn't been initialised yet");
        }
        return instance;
    }

    public final Database db;
    public final DatabaseConfig config;
    public final VerifierApp verifierApp;
    public final WorkerApp workerApp;
    public final LogViewAdapter logViewAdapter;

    public CurrentTestState(Database db, DatabaseConfig config, VerifierApp verifierApp, WorkerApp workerApp, LogViewAdapter logViewAdapter) {
        this.db = db;
        this.config = config;
        this.verifierApp = verifierApp;
        this.workerApp = workerApp;
        this.logViewAdapter = logViewAdapter;
    }

    public static void init(Database db, DatabaseConfig config, VerifierApp verifierApp, WorkerApp workerApp, LogViewAdapter logViewAdapter) {
        instance = new CurrentTestState(db, config, verifierApp, workerApp, logViewAdapter);
    }

    public static void clear() {
        instance = null;
    }

    public static boolean hasInstance() {
        return instance != null;
    }
}
