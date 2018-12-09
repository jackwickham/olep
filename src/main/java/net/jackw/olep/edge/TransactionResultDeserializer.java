package net.jackw.olep.edge;

import com.fasterxml.jackson.databind.module.SimpleModule;
import net.jackw.olep.common.JsonDeserializer;

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
