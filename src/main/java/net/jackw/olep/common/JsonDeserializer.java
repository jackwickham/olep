package net.jackw.olep.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private Class<T> destinationClass;
    protected final ObjectMapper objectMapper;

    /**
     * Construct a new deserializer. After constructing with this, and before using it,
     * {@link #configure(Map, boolean)} should be called with destinationClass populated
     */
    public JsonDeserializer() {
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
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        destinationClass = (Class<T>) configs.get("destinationClass");
    }

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
            return null;
        }

        try {
            return objectMapper.readValue(data, destinationClass);
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
}
