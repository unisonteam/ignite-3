package org.apache.ignite.rest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.gson.Gson;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.rest.presentation.ConfigurationPresentationTest;
import org.apache.ignite.rest.presentation.TestRootConfiguration;
import org.apache.ignite.rest.presentation.hocon.HoconPresentation;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The base test for configuration controllers.
 */
@MicronautTest
public abstract class ConfigurationControllerBaseTest {
    @Inject
    EmbeddedServer server;
    
    @Inject
    ConfigurationPresentation<String> cfgPresentation;
    
    @Inject
    ConfigurationRegistry configurationRegistry;
    
    @Inject
    ApplicationContext context;
    
    abstract HttpClient client();
    
    @BeforeEach
    void beforeEach() throws Exception {
        var cfg = configurationRegistry.getConfiguration(TestRootConfiguration.KEY);
        cfg.change(c -> c.changeFoo("foo").changeSubCfg(subCfg -> subCfg.changeBar("bar"))).get(1, SECONDS);
    }
    
    @Test
    void testGetConfig() {
        var response = client().toBlocking().exchange("", String.class);
        
        assertEquals(HttpStatus.OK, response.status());
        assertEquals(cfgPresentation.represent(), response.body());
    }
    
    @Test
    void testGetConfigByPath() {
        var response = client().toBlocking().exchange("/root.subCfg", String.class);
        
        assertEquals(HttpStatus.OK, response.status());
        assertEquals(cfgPresentation.representByPath("root.subCfg"), response.body());
    }
    
    @Test
    void testUpdateConfig() {
        String givenChangedConfig = "{\"root\":{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"changed\"}}}";
        
        var response = client().toBlocking().exchange(HttpRequest.PUT("", givenChangedConfig));
        assertEquals(response.status(), HttpStatus.OK);
        
        String changedConfigValue = client().toBlocking().exchange("/root.subCfg.bar", String.class).body();
        assertEquals("\"changed\"", changedConfigValue);
    }
    
    @Test
    void testUnrecognizedConfigPath() {
        try {
            client().toBlocking().exchange("/no-such-root.some-value");
            fail("Expected exception to be thrown");
        } catch (HttpClientResponseException exception) {
            assertEquals(HttpStatus.BAD_REQUEST, exception.getResponse().status());
            var errorResult = getErrorResult(exception);
            
            assertEquals("CONFIG_PATH_UNRECOGNIZED", errorResult.type());
            assertTrue(errorResult.message().contains("no-such-root"));
        }
    }
    
    @Test
    void testUnrecognizedConfigPathForUpdate() {
        String givenBrokenConfig = "{\"root\":{\"foo\":\"foo\",\"subCfg\":{\"no-such-bar\":\"bar\"}}}";
        try {
            client().toBlocking().exchange(HttpRequest.PUT("", givenBrokenConfig));
            fail("Expected exception to be thrown");
        } catch (HttpClientResponseException exception) {
            assertEquals(HttpStatus.BAD_REQUEST, exception.getResponse().status());
            var errorResult = getErrorResult(exception);
            
            assertEquals("INVALID_CONFIG_FORMAT", errorResult.type());
            assertTrue(errorResult.message().contains("no-such-bar"));
        }
    }
    
    @Test
    void testValidationForUpdate() {
        String givenConfigWithError = "{\"root\":{\"foo\":\"error\",\"subCfg\":{\"bar\":\"bar\"}}}";
        try {
            client().toBlocking().exchange(HttpRequest.PUT("", givenConfigWithError));
            fail("Expected exception to be thrown");
        } catch (HttpClientResponseException exception) {
            assertEquals(HttpStatus.BAD_REQUEST, exception.getResponse().status());
            var errorResult = getErrorResult(exception);
            
            assertEquals("VALIDATION_EXCEPTION", errorResult.type());
            assertTrue(errorResult.message().contains("Error word"));
        }
    }
    
    
    @NotNull
    private ErrorResult getErrorResult(HttpClientResponseException exception) {
        return exception.getResponse().getBody(String.class)
                .map(body -> new Gson().fromJson(body, ErrorResult.class)).orElseThrow();
    }
}