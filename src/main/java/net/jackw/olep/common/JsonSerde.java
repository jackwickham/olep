package net.jackw.olep.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
    private Class<T> destinationClass;

    public JsonSerde() { }

    public JsonSerde(Class<T> c) {
        destinationClass = c;
    }
    /**
     * Configure this class, which will configure the underlying serializer and deserializer.
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
     * Close this serde class, which will close the underlying serializer and deserializer.
     * This method has to be idempotent because it might be called multiple times.
     */
    @Override
    public void close() { }

    @Override
    public Serializer<T> serializer() {
        return new JsonSerializer<>(destinationClass);
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDeserializer<>(destinationClass);
    }
}
