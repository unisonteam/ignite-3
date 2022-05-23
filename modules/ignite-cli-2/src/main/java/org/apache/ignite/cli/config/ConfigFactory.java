package org.apache.ignite.cli.config;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class ConfigFactory {
    @Singleton
    public Config createConfigFromFile() {
        return new Config();
    }
}
