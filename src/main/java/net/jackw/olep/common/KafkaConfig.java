package net.jackw.olep.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.net.URL;

public class KafkaConfig {
    // Transaction flow topics
    public static final String TRANSACTION_REQUEST_TOPIC = "transaction-input";
    public static final String TRANSACTION_RESULT_TOPIC = "transaction-result";
    public static final String ACCEPTED_TRANSACTION_TOPIC = "accepted-transactions";
    public static final String MODIFICATION_LOG = "modification-log";

    // Shared stores
    public static final String ITEM_IMMUTABLE_TOPIC = "items";
    public static final String WAREHOUSE_IMMUTABLE_TOPIC = "warehouse-immutable";
    public static final String DISTRICT_IMMUTABLE_TOPIC = "district-immutable";
    public static final String CUSTOMER_IMMUTABLE_TOPIC = "customer-immutable";
    public static final String STOCK_IMMUTABLE_TOPIC = "stock-immutable";

    // Worker-local stores
    public static final String DISTRICT_NEXT_ORDER_ID_STORE = "district-next-order-id";
    public static final String STOCK_QUANTITY_STORE = "stock-quantity";
    public static final String NEW_ORDER_STORE = "new-orders";
    public static final String CUSTOMER_MUTABLE_STORE = "customer-mutable";

    public static final String DISTRICT_NEXT_ORDER_ID_CHANGELOG = workerChangelog(DISTRICT_NEXT_ORDER_ID_STORE);
    public static final String STOCK_QUANTITY_CHANGELOG = workerChangelog(STOCK_QUANTITY_STORE);
    public static final String NEW_ORDER_CHANGELOG = workerChangelog(NEW_ORDER_STORE);
    public static final String CUSTOMER_MUTABLE_CHANGELOG = workerChangelog(CUSTOMER_MUTABLE_STORE);

    private static String workerChangelog(String store) {
        return "worker-" + store + "-changelog";
    }

    // The number of records of each type is populated from the config file
    @JsonProperty(required = true)
    private int itemCount;
    @JsonProperty(required = true)
    private int warehouseCount;
    @JsonProperty(required = true)
    private int districtsPerWarehouse;
    @JsonProperty(required = true)
    private int customersPerDistrict;
    @JsonProperty(required = true)
    private int customerNameRange;
    @JsonProperty
    private boolean predictableData = false;


    private KafkaConfig() { }

    /**
     * The number of valid item IDs
     */
    public static int itemCount() {
        return getInstance().itemCount;
    }

    /**
     * The number of warehouses that the database is populated with
     */
    public static int warehouseCount() {
        return getInstance().warehouseCount;
    }

    /**
     * The number of populated districts per warehouse
     */
    public static int districtsPerWarehouse() {
        return getInstance().districtsPerWarehouse;
    }

    /**
     * The number of customers populated per district
     */
    public static int customersPerDistrict() {
        return getInstance().customersPerDistrict;
    }

    /**
     * The range of names that should be used (if < customersPerDistrict, there will be collisions, which is required
     * for TPC-C)
     */
    public static int customerNameRange() {
        return getInstance().customerNameRange;
    }

    /**
     * Whether the database should be populated deterministically
     */
    public static boolean predictableData() {
        return getInstance().predictableData;
    }


    private static KafkaConfig instance = null;

    private static KafkaConfig getInstance() {
        if (instance == null) {
            // Load from config file on class path
            try {
                URL configFile;
                // If we're running a test, use that database config. This takes precedence over everything else
                configFile = KafkaConfig.class.getClassLoader().getResource("test-database-config.yml");
                if (configFile == null) {
                    String prodEnv = System.getenv("PRODUCTION");
                    if (prodEnv == null || prodEnv.equals("") || prodEnv.equals("0")) {
                        // Normally use the default config file
                        configFile = KafkaConfig.class.getClassLoader().getResource("database-config.yml");
                    } else {
                        // If this is production, as set by the PRODUCTION env variable, use the production config file
                        configFile = KafkaConfig.class.getClassLoader().getResource("prod-database-config.yml");
                    }
                }
                instance = new ObjectMapper(new YAMLFactory()).readValue(configFile, KafkaConfig.class);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load config file", e);
            }
        }
        return instance;
    }
}
