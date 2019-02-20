package net.jackw.olep.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Arguments {
    private DatabaseConfig config;
    private Map<String, String> arguments;
    private Map<String, Boolean> flags;

    public Arguments(String[] args) throws IOException {
        arguments = new HashMap<>();
        flags = new HashMap<>();
        String databaseConfigFile = null;

        populateDefaults();

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                if (arguments.containsKey(key)) {
                    if (args.length > ++i) {
                        arguments.put(key, args[i]);
                    } else {
                        throw new RuntimeException(String.format("Argument --%s requires a value", key));
                    }
                } else if (flags.containsKey(key)) {
                    flags.put(key, true);
                } else {
                    throw new RuntimeException("Unknown argument " + args[i]);
                }
            } else if (args.length == i+1) {
                // last argument
                databaseConfigFile = args[i];
            } else {
                throw new RuntimeException("Unknown argument " + args[i]);
            }
        }

        if (databaseConfigFile == null) {
            config = DatabaseConfig.create(getCallingClass());
        } else {
            config = DatabaseConfig.create(databaseConfigFile, getCallingClass());
        }
    }

    private void populateDefaults() {
        arguments.putAll(Map.of("ready-file", ""));
        flags.putAll(Map.of("all", false));
    }

    public String getReadyFileArg() {
        return arguments.get("ready-file");
    }

    public boolean getAllFlag() {
        return flags.get("all");
    }

    public DatabaseConfig getConfig() {
        return config;
    }

    private static String getCallingClass() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        // Start at 3 to exclude .getStackTrace(), .getCallingClass() and the local caller
        for (int i = 3; i < trace.length; i++) {
            String className = trace[i].getClassName();
            if (!className.equals(DatabaseConfig.class.getName()) && !className.equals(Arguments.class.getName())) {
                return className.substring(className.lastIndexOf('.') + 1);
            }
        }
        throw new RuntimeException("Failed to find calling method name");
    }
}
