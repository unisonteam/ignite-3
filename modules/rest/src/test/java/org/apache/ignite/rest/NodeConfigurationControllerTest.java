package org.apache.ignite.rest;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.rest.presentation.PresentationsFactory;
import org.apache.ignite.rest.presentation.hocon.HoconPresentation;

/**
 * Functional test for {@link NodeConfigurationController}.
 */
@MicronautTest
class NodeConfigurationControllerTest extends ConfigurationControllerBaseTest {

    @Inject
    @Client("/management/v1/configuration/node/")
    HttpClient client;

    @Override
    HttpClient client() {
        return client;
    }

    /**
     * Creates test hocon configuration representation.
     */
    @MockBean
    @Named("nodeCfgPresentation")
    @Replaces(factory = PresentationsFactory.class)
    public ConfigurationPresentation<String> cfgPresentation(ConfigurationRegistry configurationRegistry) {
        return new HoconPresentation(configurationRegistry);
    }
}