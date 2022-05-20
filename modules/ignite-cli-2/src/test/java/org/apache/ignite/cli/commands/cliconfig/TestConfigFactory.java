package org.apache.ignite.cli.commands.cliconfig;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;
import java.util.Properties;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.config.ConfigFactory;

@Factory
@Replaces(factory = ConfigFactory.class)
public class TestConfigFactory {
    @Singleton
    public Config createConfig() {
        return createTestConfig();
    }

    static Config createTestConfig() {
        Properties p = new Properties();
        p.setProperty("ignite.cluster-url", "test_cluster_url");
        p.setProperty("ignite.jdbc-url", "test_jdbc_url");
        return new Config(p);
    }
}
