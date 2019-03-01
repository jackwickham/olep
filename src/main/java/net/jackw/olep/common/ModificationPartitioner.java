package net.jackw.olep.common;

import net.jackw.olep.message.modification.ModificationKey;
import net.jackw.olep.message.modification.ModificationMessage;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class ModificationPartitioner implements StreamPartitioner<ModificationKey, ModificationMessage> {
    @Override
    public Integer partition(String topic, ModificationKey key, ModificationMessage value, int numPartitions) {
        return value.getViewWarehouse() % numPartitions;
    }
}
