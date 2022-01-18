package org.apache.ignite.rest;

import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

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
}