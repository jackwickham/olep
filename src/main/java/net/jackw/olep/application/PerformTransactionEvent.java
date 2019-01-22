package net.jackw.olep.application;

public enum PerformTransactionEvent {
    NEW_ORDER    (18, 12),
    PAYMENT      (3, 12),
    DELIVERY     (2, 5),
    ORDER_STATUS (2, 10),
    STOCK_LEVEL  (2, 5);

    public final double keyingTime;
    public final double thinkTime;

    private PerformTransactionEvent(double keyingTime, double thinkTime) {
        this.keyingTime = keyingTime;
        this.thinkTime = thinkTime;
    }
}
