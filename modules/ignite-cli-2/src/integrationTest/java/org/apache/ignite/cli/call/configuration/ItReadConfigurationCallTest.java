package org.apache.ignite.cli.call.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.CallIntegrationTestBase;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ReadConfigurationCall}.
 */
class ItReadConfigurationCallTest extends CallIntegrationTestBase {

    @Inject
    ReadConfigurationCall call;

    @Test
    @DisplayName("Should read configuration when cluster up and running")
    void readDefaultConfiguration() {
        // Given
        ReadConfigurationCallInput input = ReadConfigurationCallInput.builder()
                .clusterUrl(CLUSTER_URL)
                .build();

        // When
        DefaultCallOutput output = call.execute(input);

        // Then
        assertThat(output).isEqualTo(DefaultCallOutput.success("Read..")); // fixme
    }
}