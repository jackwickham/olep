package net.jackw.olep.verifier;

import net.jackw.olep.message.TransactionResultMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ResultProcessor implements Processor<Long, Boolean> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(Long transactionId, Boolean accepted) {
        TransactionResultMessage result = new TransactionResultMessage(transactionId, accepted);
        context.forward(transactionId, result);
    }

    @Override
    public void close() {

    }
}
