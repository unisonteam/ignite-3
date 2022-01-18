package org.apache.ignite.rest;

import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

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
}