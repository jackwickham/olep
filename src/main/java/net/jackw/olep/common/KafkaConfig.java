package net.jackw.olep.common;

public class KafkaConfig {
    // Transaction flow topics
    public static final String TRANSACTION_REQUEST_TOPIC = "transaction-input";
    public static final String TRANSACTION_RESULT_TOPIC = "transaction-result";
    public static final String ACCEPTED_TRANSACTION_TOPIC = "accepted-transactions";

    // Shared stores
    public static final String ITEM_IMMUTABLE_TOPIC = "items";
    public static final String WAREHOUSE_IMMUTABLE_TOPIC = "warehouse-immutable";
    public static final String DISTRICT_IMMUTABLE_TOPIC = "district-immutable";
    public static final String CUSTOMER_IMMUTABLE_TOPIC = "customer-immutable";
    public static final String STOCK_IMMUTABLE_TOPIC = "stock-immutable";

    private KafkaConfig() { }
}
