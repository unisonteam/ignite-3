package org.apache.ignite.cli.commands.cliconfig;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.IOException;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.config.ConfigFactory;

@Factory
@Replaces(factory = ConfigFactory.class)
public class TestConfigFactory {
    @Singleton
    public Config createConfig() throws IOException {
        return createTestConfig();
    }

    static Config createTestConfig() throws IOException {
        File tempFile = File.createTempFile("cli", null);
        tempFile.deleteOnExit();
        Config config = new Config(tempFile);
        config.setProperty("ignite.cluster-url", "test_cluster_url");
        config.setProperty("ignite.jdbc-url", "test_jdbc_url");
        return config;
    }
}
