package net.jackw.olep.common;

import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class DatabaseConfigTest {
    @Test
    public void testBootstrapServersDefault() throws IOException {
        DatabaseConfig config = DatabaseConfig.create("test");
        assertEquals("127.0.0.1:9092", config.getBootstrapServers());
    }

    @Test
    public void testBootstrapServersOne() throws IOException {
        DatabaseConfig config = DatabaseConfig.create(getPath("one-bootstrap-server-test-config.yml"), "test");
        assertEquals("127.8.9.10", config.getBootstrapServers());
    }

    @Test
    public void testBootstrapServersMultiple() throws IOException {
        DatabaseConfig config = DatabaseConfig.create(getPath("multiple-bootstrap-servers-test-config.yml"), "test");
        assertEquals("127.8.9.10,10.1.1.1", config.getBootstrapServers());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetMysqlUserWhenNoConfig() throws IOException {
        Assume.assumeTrue(System.getenv("MYSQL_USER") == null);
        DatabaseConfig config = DatabaseConfig.create("test");
        config.getMysqlUser();
    }

    @Test
    public void testGetMysqlUserWhenConfigured() throws IOException {
        DatabaseConfig config = DatabaseConfig.create(getPath("mysql-test-config.yml"), "test");
        assertEquals("theuser", config.getMysqlUser());
    }

    private String getPath(String configFile) {
        return DatabaseConfigTest.class.getClassLoader().getResource(configFile).getPath();
    }
}
