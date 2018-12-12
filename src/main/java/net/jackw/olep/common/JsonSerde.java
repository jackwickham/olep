package net.jackw.olep.common;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
    private JsonSerializer<T> serializer;
    private JsonDeserializer<T> deserializer;

    public JsonSerde() { }

    public JsonSerde(Class<T> c) {
        serializer = new JsonSerializer<>();
        deserializer = new JsonDeserializer<>(c);
    }

    public JsonSerde(TypeReference<T> t) {
        serializer = new JsonSerializer<>();
        deserializer = new JsonDeserializer<>(t);
    }

    /**
     * Configure this class, which will configure the underlying serializer and deserializer.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    /**
     * Close this serde class, which will close the underlying serializer and deserializer.
     * This method has to be idempotent because it might be called multiple times.
     */
    @Override
    public void close() { }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
