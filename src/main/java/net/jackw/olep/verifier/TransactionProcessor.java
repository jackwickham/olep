package net.jackw.olep.verifier;

import net.jackw.olep.message.TestMessage;
import net.jackw.olep.message.TransactionRequestMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

public class TransactionProcessor implements Processor<Long, TransactionRequestMessage> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    /**
     * Process the record with the given key and value.
     *
     * @param key   the key for the record
     * @param value the value for the record
     */
    @Override
    public void process(Long key, TransactionRequestMessage value) {
        System.out.printf("Processing transaction %d\n", key);
        if (value.body instanceof TestMessage) {
            context.forward(key, value, To.child("accepted-transactions"));
            context.forward(key, true, To.child("results"));
            System.out.println("Accepted a test message");
        } else {
            context.forward(key, false, To.child("return-status"));
            System.out.println("Rejected a messaged of type " + value.body.getClass().getName());
        }
    }

    @Override
    public void close() {

    }
}
