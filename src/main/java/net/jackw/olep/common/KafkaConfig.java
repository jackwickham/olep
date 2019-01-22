package net.jackw.olep.common;

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

    private KafkaConfig() { }

    private static String workerChangelog(String store) {
        return "worker-" + store + "-changelog";
    }
}
