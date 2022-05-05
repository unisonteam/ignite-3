package org.apache.ignite.cli.call.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.CallIntegrationTestBase;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ShowConfigurationCall}.
 */
class ItShowConfigurationCallTest extends CallIntegrationTestBase {

    @Inject
    ShowConfigurationCall call;

    @Test
    @DisplayName("Should show cluster configuration when cluster up and running")
    void readClusterConfiguration() {
        // Given
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl(NODE_URL)
                .build();

        // When
        DefaultCallOutput<String> output = call.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body()).isNotEmpty();
    }

    @Test
    @DisplayName("Should show cluster configuration by path when cluster up and running")
    void readClusterConfigurationByPath() {
        // Given
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl(NODE_URL)
                .selector("rocksDb.defaultRegion.cache")
                .build();

        // When
        DefaultCallOutput<String> output = call.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body()).isEqualTo("\"lru\"");
    }
}