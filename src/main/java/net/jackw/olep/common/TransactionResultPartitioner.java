package net.jackw.olep.common;

import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class TransactionResultPartitioner implements StreamPartitioner<Long, TransactionResultMessage> {
    @Override
    public Integer partition(String topic, Long key, TransactionResultMessage value, int numPartitions) {
        return (int)((key & 0xFFFFFFFFL) % numPartitions);
    }
}
