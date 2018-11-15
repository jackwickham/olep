package net.jackw.olep.edge.transaction_result;

import com.fasterxml.jackson.databind.module.SimpleModule;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.edge.PendingTransaction;

import java.util.Map;

public class TransactionResultDeserializer extends JsonDeserializer<PendingTransaction> {
    public TransactionResultDeserializer(Map<Long, PendingTransaction<?, ?>> pendingTransactionMap) {
        super(PendingTransaction.class);

        // Register the transaction result deserializer (which deserializes transaction results to PendingTransactions)
        SimpleModule module = new SimpleModule();
        module.addDeserializer(PendingTransaction.class, new TransactionResultJsonDeserializer(pendingTransactionMap, this.objectMapper));
        this.objectMapper.registerModule(module);
    }
}
