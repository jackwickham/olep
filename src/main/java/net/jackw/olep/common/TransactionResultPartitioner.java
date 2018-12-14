package net.jackw.olep.common;

import net.jackw.olep.message.transaction_result.TransactionResultKey;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class TransactionResultPartitioner implements StreamPartitioner<TransactionResultKey, Object> {
    @Override
    public Integer partition(String topic, TransactionResultKey key, Object value, int numPartitions) {
        return partition(key.getConnectionId(), numPartitions);
    }

    public static int partition(int connectionId, int numPartitions) {
        int partition = connectionId % numPartitions;
        if (partition < 0) {
            partition += numPartitions;
        }
        return partition;
    }
}
