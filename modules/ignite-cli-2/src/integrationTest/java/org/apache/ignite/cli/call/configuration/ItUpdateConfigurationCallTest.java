package org.apache.ignite.cli.call.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.CallIntegrationTestBase;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link UpdateConfigurationCall}.
 */
public class ItUpdateConfigurationCallTest extends CallIntegrationTestBase {

    @Inject
    UpdateConfigurationCall updateCall;

    @Inject
    ShowConfigurationCall readCall;

    @Test
    @DisplayName("Should show cluster configuration")
    void shouldUpdateClusterConfiguration() {
        // Given default write buffer size
        String givenConfigurationProperty = readConfigurationProperty("rocksDb.defaultRegion.writeBufferSize");
        assertThat(givenConfigurationProperty).isEqualTo("67108864");
        // And
        var input = UpdateConfigurationCallInput.builder()
                .clusterUrl(NODE_URL)
                .config("{rocksDb: {defaultRegion: {writeBufferSize: 1024}}}")
                .build();

        // When update buffer size
        DefaultCallOutput<String> output = updateCall.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body()).isEmpty();
        // And buffer size is updated
        String updatedConfigurationProperty = readConfigurationProperty("rocksDb.defaultRegion.writeBufferSize");
        assertThat(updatedConfigurationProperty).isEqualTo("1024");

        // When update buffer size back to default but using key-value format
        updateCall.execute(
                UpdateConfigurationCallInput.builder()
                        .clusterUrl(NODE_URL)
                        .config("rocksDb.defaultRegion.writeBufferSize=67108864")
                        .build()
        );

        // Then buffer size is updated
        assertThat(readConfigurationProperty("rocksDb.defaultRegion.writeBufferSize")).isEqualTo("67108864");
    }

    private String readConfigurationProperty(String selector) {
        var input = ShowConfigurationCallInput.builder().clusterUrl(NODE_URL).selector(selector).build();
        return readCall.execute(input).body();
    }
}
