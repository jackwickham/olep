package net.jackw.olep.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private Class<T> destinationClass;
    private TypeReference<T> type;
    private final ObjectMapper objectMapper;

    /**
     * Construct a new deserializer. After constructing with this, and before using it,
     * {@link #configure(Map, boolean)} should be called with destinationClass populated
     */
    private JsonDeserializer() {
        objectMapper = new ObjectMapper().registerModule(new GuavaModule());
    }

    /**
     * Construct a new deserializer for a particular class
     *
     * @param c The class to deserialize to
     */
    public JsonDeserializer(Class<T> c) {
        this();
        this.destinationClass = c;
    }

    /**
     * Construct a new deserializer, providing a TypeReference to help with deserialization
     *
     * The TypeReference should be constructed as {@code new TypeReference<ConcreteType>() {} } (with no generic
     * parameters present)
     *
     * @param t The TypeReference that corresponds to the value that should be deserialized
     */
    public JsonDeserializer(TypeReference<T> t) {
        this();
        type = t;
    }

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            log.warn("Deserialized null from {}", topic);
            return null;
        }

        return deserialize(data);
    }

    /**
     * Deserialize a record from a byte array into a new object
     *
     * @param data The serialized bytes
     * @return Deserialized data
     */
    public T deserialize(@Nonnull byte[] data) {
        try {
            if (type != null) {
                // Use the TypeReference if possible
                return objectMapper.readValue(data, type);
            } else {
                return objectMapper.readValue(data, destinationClass);
            }
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Deserialize a record from a byte array and update an existing object with the results
     *
     * @param data The serialized bytes
     * @param target The object to deserialize into
     * @return target
     */
    @CanIgnoreReturnValue
    public T deserialize(byte[] data, T target) {
        if (data == null) {
            log.warn("Deserialized null");
            return null;
        }

        try {
            return objectMapper.readerForUpdating(target).readValue(data);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Close this deserializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    public void close() { }

    private static Logger log = LogManager.getLogger();
}
