package net.jackw.olep.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Test;

import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class JsonDeserializerTest {
    @Test
    public void testDeserializationWithClassRef() {
        byte[] data = "{\"n\":200}".getBytes(UTF_8);
        JsonDeserializer<Target> deserializer = new JsonDeserializer<>(Target.class);

        Target result = deserializer.deserialize(data);

        assertEquals(new Target(200, null), result);
    }

    @Test
    public void testDeserializationWithTypeReference() {
        byte[] data = "[{\"n\":200}]".getBytes(UTF_8);
        JsonDeserializer<ImmutableList<Target>> deserializer = new JsonDeserializer<>(new TypeReference<ImmutableList<Target>>() {});

        ImmutableList<Target> result = deserializer.deserialize(data);

        assertEquals(ImmutableList.of(new Target(200, null)), result);
    }

    @Test
    public void testDeserializationWithTopicReturnsNullWhenGivenNull() {
        JsonDeserializer<Target> deserializer = new JsonDeserializer<>(Target.class);

        Target result = deserializer.deserialize("topic", null);
        assertNull(result);
    }

    @Test
    public void testDeserializationWithUpdate() {
        Target t = new Target(200, null);
        byte[] data = "{\"s\":\"Hello, world\"}".getBytes(UTF_8);
        JsonDeserializer<Target> deserializer = new JsonDeserializer<>(Target.class);

        Target result = deserializer.deserialize(data, t);

        assertSame(result, t);
        assertEquals(new Target(200, "Hello, world"), t);
    }

    @Test
    public void testJsonSerdeReturnsWorkingDeserializerFromClass() {
        byte[] data = "{\"n\":200}".getBytes(UTF_8);
        Deserializer<Target> deserializer = new JsonSerde<>(Target.class).deserializer();

        Target result = deserializer.deserialize("topic", data);

        assertEquals(new Target(200, null), result);
    }

    @Test
    public void testJsonSerdeReturnsWorkingDeserializerFromTypeReference() {
        byte[] data = "{\"n\":200}".getBytes(UTF_8);
        Deserializer<Target> deserializer = new JsonSerde<>(new TypeReference<Target>() {}).deserializer();

        Target result = deserializer.deserialize("topic", data);

        assertEquals(new Target(200, null), result);
    }

    @SuppressWarnings("EqualsHashCode")
    public static class Target {
        public Integer n = 100;
        public String s = null;

        public Target() {}

        public Target(Integer n, String s) {
            this.n = n;
            this.s = s;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Target) {
                return Objects.equals(n, ((Target) obj).n) && Objects.equals(s, ((Target) obj).s);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("n", n)
                .add("s", s)
                .toString();
        }
    }
}
