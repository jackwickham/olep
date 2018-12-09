package net.jackw.olep.edge;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.jackw.olep.edge.PendingTransaction;
import net.jackw.olep.message.transaction_result.TransactionResultBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * @link https://www.baeldung.com/jackson-deserialization
 */
public class TransactionResultJsonDeserializer extends StdDeserializer<PendingTransaction<?, ?>> {
    private final Map<Long, PendingTransaction<?, ?>> pendingTransactionMap;
    private final ObjectMapper objectMapper;

    public TransactionResultJsonDeserializer(Map<Long, PendingTransaction<?, ?>> pendingTransactionMap, ObjectMapper objectMapper) {
        this(pendingTransactionMap, objectMapper, null);
    }

    public TransactionResultJsonDeserializer(Map<Long, PendingTransaction<?, ?>> pendingTransactionMap, ObjectMapper objectMapper, Class<?> vc) {
        super(vc);
        this.pendingTransactionMap = pendingTransactionMap;
        this.objectMapper = objectMapper;
    }

    /**
     * Method that can be called to ask implementation to deserialize
     * JSON content into the value type this serializer handles.
     * Returned instance is to be constructed by method itself.
     * <p>
     * Pre-condition for this method is that the parser points to the
     * first event that is part of value to deserializer (and which
     * is never JSON 'null' literal, more on this below): for simple
     * types it may be the only value; and for structured types the
     * Object start marker or a FIELD_NAME.
     * </p>
     * <p>
     * The two possible input conditions for structured types result
     * from polymorphism via fields. In the ordinary case, Jackson
     * calls this method when it has encountered an OBJECT_START,
     * and the method implementation must advance to the next token to
     * see the first field name. If the application configures
     * polymorphism via a field, then the object looks like the following.
     * <pre>
     *      {
     *          "@class": "class name",
     *          ...
     *      }
     *  </pre>
     * Jackson consumes the two tokens (the <tt>@class</tt> field name
     * and its value) in order to learn the class and select the deserializer.
     * Thus, the stream is pointing to the FIELD_NAME for the first field
     * after the @class. Thus, if you want your method to work correctly
     * both with and without polymorphism, you must begin your method with:
     * <pre>
     *       if (p.getCurrentToken() == JsonToken.START_OBJECT) {
     *         p.nextToken();
     *       }
     *  </pre>
     * This results in the stream pointing to the field name, so that
     * the two conditions align.
     * <p>
     * Post-condition is that the parser will point to the last
     * event that is part of deserialized value (or in case deserialization
     * fails, event that was not recognized or usable, which may be
     * the same event as the one it pointed to upon call).
     * <p>
     * Note that this method is never called for JSON null literal,
     * and thus deserializers need (and should) not check for it.
     *
     * @param p    Parsed used for reading JSON content
     * @param ctxt Context that can be used to access information about
     *             this deserialization activity.
     * @return Deserialized value
     */
    @Override
    public PendingTransaction<?, ?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        // Load the correct pending transaction
        long transactionId = node.get("transactionId").longValue();
        PendingTransaction<?, ?> pendingTransaction = pendingTransactionMap.get(transactionId);
        if (pendingTransaction == null) {
            throw new IllegalStateException("Tried to decode a transaction that wasn't in the pending transactions map");
        }

        // Update the pending status
        JsonNode approvedNode = node.get("approved");
        if (approvedNode instanceof BooleanNode) {
            pendingTransaction.setAccepted(approvedNode.booleanValue());
        }

        // Update the transaction result builder with these new results, and notify the pending transaction that we have done so
        JsonNode resultsNode = node.get("results");
        if (resultsNode instanceof ObjectNode) {
            TransactionResultBuilder<?> builder = pendingTransaction.getTransactionResultBuilder();
            objectMapper.readerForUpdating(builder).readValue(resultsNode);
            pendingTransaction.builderUpdated();
        }

        return pendingTransaction;
    }


}
