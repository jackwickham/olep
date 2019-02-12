package net.jackw.olep.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class DatabaseConfig {
    private DatabaseConfig() { }

    @JsonProperty(required = true)
    private int itemCount;

    @JsonProperty(required = true)
    private int warehouseCount;

    @JsonProperty
    private int districtsPerWarehouse = 10;

    @JsonProperty(required = true)
    private int customersPerDistrict;

    @JsonProperty(required = true)
    private int customerNameRange;

    @JsonProperty(required = true)
    private int verifierInstances;

    @JsonProperty(required = true)
    private int workerInstances;

    @JsonProperty
    private int applicationInstances = 1;

    @JsonProperty
    private int verifierThreads = 2;

    @JsonProperty
    private int workerThreads = 2;

    @JsonProperty
    private int viewThreads = 1;

    @JsonProperty
    private String storeBackingDir = "/tmp/olep/";

    @JsonProperty
    private boolean predictableData = false;

    @JsonProperty
    private String bootstrapServers = "127.0.0.1:9092";

    @JsonProperty
    private String viewRegistryHost = "127.0.0.1";

    /**
     * Get the number of items that the database will hold
     */
    public int getItemCount() {
        return itemCount;
    }

    /**
     * Get the number of warehouses
     */
    public int getWarehouseCount() {
        return warehouseCount;
    }

    /**
     * Get the number of districts associated with each warehouse
     */
    public int getDistrictsPerWarehouse() {
        return districtsPerWarehouse;
    }

    /**
     * Get the number of customers associated with each district
     */
    public int getCustomersPerDistrict() {
        return customersPerDistrict;
    }

    /**
     * Get the number of distinct customer surnames that will be generated
     */
    public int getCustomerNameRange() {
        return customerNameRange;
    }

    /**
     * Get the number of verifier instances in the database system
     */
    public int getVerifierInstances() {
        return verifierInstances;
    }

    /**
     * Get the number of worker instances in the database system
     */
    public int getWorkerInstances() {
        return workerInstances;
    }

    /**
     * Get the number of application instances (instances of net.jackw.olep.application.App) that will be using the
     * database
     */
    public int getApplicationInstances() {
        return applicationInstances;
    }

    /**
     * Get the number of verifier threads that should be created per application instance
     */
    public int getVerifierThreads() {
        return verifierThreads;
    }

    /**
     * Get the number of worker threads that should be created per application instance
     */
    public int getWorkerThreads() {
        return workerThreads;
    }

    /**
     * Get the number of view threads that should be created per application instance
     */
    public int getViewThreads() {
        return viewThreads;
    }

    /**
     * Get the directory where disk-backed stores should be placed
     */
    public String getStoreBackingDir() {
        return storeBackingDir;
    }

    /**
     * Should the data be generated deterministically, to allow exact reproducibility?
     */
    public boolean isPredictableData() {
        return predictableData;
    }

    /**
     * Get the Kafka bootstrap servers
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Get the Java RMI registry where the views are registered
     */
    public String getViewRegistryHost() {
        return viewRegistryHost;
    }

    /**
     * Load the database config from the correct configuration file
     *
     * A config file can be provided as the first command line argument. If it is omitted, the config file is loaded
     * from the classpath, using test-database-config.yml if it's available, and database-config.yml if not.
     *
     * The config file should be a yml file.
     *
     * @param cmdArgs The command line arguments to the program
     * @return The loaded database configuration
     * @throws FileNotFoundException If the config file from the command line doesn't exist, or the default config file
     *                               can't be loaded
     * @throws IOException If the file can't be read
     */
    public static DatabaseConfig create(List<String> cmdArgs) throws IOException {
        InputStream configFile = null;
        try {
            if (cmdArgs.size() > 0) {
                configFile = new FileInputStream(cmdArgs.get(0));
            } else {
                configFile = KafkaConfig.class.getClassLoader().getResourceAsStream("test-database-config.yml");
                if (configFile == null) {
                    configFile = KafkaConfig.class.getClassLoader().getResourceAsStream("database-config.yml");
                    if (configFile == null) {
                        throw new FileNotFoundException("Failed to load database-config.yml from classpath");
                    }
                }
            }

            return new ObjectMapper(new YAMLFactory()).readValue(configFile, DatabaseConfig.class);
        } finally {
            if (configFile != null) {
                configFile.close();
            }
        }
    }

    /**
     * @see #create(List)
     */
    public static DatabaseConfig create(String[] cmdArgs) throws IOException {
        return create(Arrays.asList(cmdArgs));
    }
}