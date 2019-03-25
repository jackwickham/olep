package net.jackw.olep.common;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ArgumentsTest {
    @Test
    public void testNoArguments() throws IOException  {
        Arguments args = new Arguments(new String[]{});
        assertEquals("", args.getReadyFileArg());
        assertFalse(args.getAllFlag());

        DatabaseConfig config = args.getConfig();
        assertThat(config.getRunId(), Matchers.containsString("ArgumentsTest"));
    }

    @Test
    public void testAllFlag() throws IOException {
        Arguments args = new Arguments(new String[]{"--all"});
        assertEquals("", args.getReadyFileArg());
        assertTrue(args.getAllFlag());

        DatabaseConfig config = args.getConfig();
        assertThat(config.getRunId(), Matchers.containsString("ArgumentsTest"));
    }

    @Test
    public void testReadyFile() throws IOException {
        Arguments args = new Arguments(new String[]{"--ready-file", "/tmp/ready-file-test"});
        assertEquals("/tmp/ready-file-test", args.getReadyFileArg());
        assertFalse(args.getAllFlag());

        DatabaseConfig config = args.getConfig();
        assertThat(config.getRunId(), Matchers.containsString("ArgumentsTest"));
    }

    @Test(expected = RuntimeException.class)
    public void testReadyFileRequiresArgument() throws IOException {
        new Arguments(new String[]{"--ready-file"});
    }

    @Test
    public void testConfigFile() throws IOException {
        String configFileLocation = ArgumentsTest.class.getClassLoader().getResource("arguments-test-config.yml").getPath();
        Arguments args = new Arguments(new String[]{configFileLocation});
        assertEquals("", args.getReadyFileArg());
        assertFalse(args.getAllFlag());

        DatabaseConfig config = args.getConfig();
        assertEquals("test value", config.getRunId());
        assertEquals(42, config.getWarehouseCount());
    }

    @Test(expected = IOException.class)
    public void testIOExceptionIfConfigFileDoesntExist() throws IOException {
        new Arguments(new String[]{"/tmp/olep-tests/THIS_FILE_DOES_NOT_EXIST.yml"});
    }

    @Test(expected = RuntimeException.class)
    public void testIllegalArgument() throws IOException {
        new Arguments(new String[]{"--non-existent-argument"});
    }

    @Test(expected = RuntimeException.class)
    public void testPassingValueToAll() throws IOException {
        new Arguments(new String[]{"--all", "val", "--ready-file", "/tmp/ready-file-test"});
    }

    @Test
    public void testAllTogether() throws IOException {
        String configFileLocation = ArgumentsTest.class.getClassLoader().getResource("arguments-test-config.yml").getPath();
        Arguments args = new Arguments(new String[]{"--ready-file", "/tmp/ready-file-test", "--all", configFileLocation});
        assertEquals("/tmp/ready-file-test", args.getReadyFileArg());
        assertTrue(args.getAllFlag());

        DatabaseConfig config = args.getConfig();
        assertEquals("test value", config.getRunId());
        assertEquals(42, config.getWarehouseCount());
    }
}
