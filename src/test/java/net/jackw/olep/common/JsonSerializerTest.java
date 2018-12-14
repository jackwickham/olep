package net.jackw.olep.common;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class JsonSerializerTest {
    @Test
    public void testSerialization() {
        Target t = new Target();
        JsonSerializer<Target> serializer = new JsonSerializer<>();

        byte[] serialized = serializer.serialize("topic", t);
        assertArrayEquals("{\"n\":100}".getBytes(UTF_8), serialized);
    }

    @Test
    public void testGuavaSerialization() {
        ImmutableList<Target> list = ImmutableList.of(new Target());
        JsonSerializer<ImmutableList<Target>> serializer = new JsonSerializer<>();

        byte[] serialized = serializer.serialize("topic", list);
        assertArrayEquals("[{\"n\":100}]".getBytes(UTF_8), serialized);
    }

    public static class Target {
        public int n = 100;
    }
}
