package net.jackw.olep.edge.transaction_result;

public abstract class TransactionResultBuilder<T extends TransactionResult> {
    public abstract boolean canBuild();
    public abstract T build();
}
