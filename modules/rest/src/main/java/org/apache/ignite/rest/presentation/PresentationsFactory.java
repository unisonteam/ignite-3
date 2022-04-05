package org.apache.ignite.rest.presentation;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.configuration.rest.presentation.ConfigurationPresentation;

/**
 * Factory that defines beans that needed for rest module.
 */
@Factory
public class PresentationsFactory {
    private final ConfigurationPresentation<String> clusterCfgPresentation;
    private final ConfigurationPresentation<String> nodeCfgPresentation;

    public PresentationsFactory(ConfigurationPresentation<String> clusterCfgPresentation,
            ConfigurationPresentation<String> nodeCfgPresentation) {
        this.clusterCfgPresentation = clusterCfgPresentation;
        this.nodeCfgPresentation = nodeCfgPresentation;
    }

    @Bean
    @Singleton
    @Named("clusterCfgPresentation")
    public ConfigurationPresentation<String> clusterCfgPresentation() {
        return clusterCfgPresentation;
    }

    @Bean
    @Singleton
    @Named("nodeCfgPresentation")
    public ConfigurationPresentation<String> nodeCfgPresentation() {
        return nodeCfgPresentation;
    }
}
