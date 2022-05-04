package org.apache.ignite.rest;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.configuration.rest.presentation.hocon.HoconPresentation;
import org.apache.ignite.rest.presentation.PresentationsFactory;

/**
 * Functional test for {@link ClusterConfigurationController}.
 */
@MicronautTest
class ClusterConfigurationControllerTest extends ConfigurationControllerBaseTest {

    @Inject
    @Client("/management/v1/configuration/cluster/")
    HttpClient client;

    @Override
    HttpClient client() {
        return client;
    }


    /**
     * Creates test hocon configuration representation.
     */
    @Bean
    @Named("clusterCfgPresentation")
    @Replaces(factory = PresentationsFactory.class)
    public ConfigurationPresentation<String> cfgPresentation(ConfigurationRegistry configurationRegistry) {
        return new HoconPresentation(configurationRegistry);
    }
}
