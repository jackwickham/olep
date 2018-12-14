package net.jackw.olep.common;

import net.jackw.olep.message.transaction_request.TransactionWarehouseKey;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class WarehousePartitioner implements StreamPartitioner<TransactionWarehouseKey, Object> {
    @Override
    public Integer partition(String topic, TransactionWarehouseKey key, Object value, int numPartitions) {
        return key.warehouseId % numPartitions;
    }
}
