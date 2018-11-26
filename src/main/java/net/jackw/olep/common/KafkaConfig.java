package net.jackw.olep.common;

public class KafkaConfig {
    public static final String TRANSACTION_REQUEST_TOPIC = "transaction-input";
    public static final String TRANSACTION_RESULT_TOPIC = "transaction-result";
    public static final String ACCEPTED_TRANSACTION_TOPIC = "accepted-transactions";
    public static final String ITEM_IMMUTABLE_TOPIC = "items";

    private KafkaConfig() { }
}
