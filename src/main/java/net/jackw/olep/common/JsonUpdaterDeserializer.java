package net.jackw.olep.common;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import net.jackw.olep.edge.transaction_result.TestResult;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public abstract class JsonUpdaterDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper;


    /**
     * Construct a new deserializer. After constructing with this, and before using it,
     * {@link #configure(Map, boolean)} should be called with target populated
     */
    public JsonUpdaterDeserializer() {
        objectMapper = new ObjectMapper().registerModule(new GuavaModule()).configure(MapperFeature.IGNORE_MERGE_FOR_UNMERGEABLE, false);
    }

    /**
     * Construct a new deserializer that will update an object
     *
     * @param tClass The class that will be updated
     */
    public JsonUpdaterDeserializer(Class<T> tClass) {
        this();
        objectMapper.configOverride(tClass).setMergeable(true);
        //objectMapper.configOverride(TestResult.Builder.class).setMergeable(true);
        objectMapper.setDefaultMergeable(true);
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
        objectMapper.configOverride((Class<T>) configs.get("destinationClass")).setMergeable(true);
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
            ObjectReader updater = objectMapper.readerForUpdating(getTarget(topic, data));
            return updater.readValue(data);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    public abstract T getTarget(String topic, byte[] data);

    /**
     * Close this deserializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    public void close() { }
}
