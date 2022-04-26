package org.apache.ignite.cli.commands.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.TestCall;
import org.apache.ignite.cli.call.configuration.UpdateConfigurationCall;
import org.apache.ignite.cli.call.configuration.UpdateConfigurationCallInput;
import org.apache.ignite.cli.commands.CommandLineBaseTest;
import org.apache.ignite.cli.core.call.Call;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UpdateConfigSubCommandTest extends CommandLineBaseTest {

    Call<UpdateConfigurationCallInput, String> mockCall = new TestCall<>();

    @BeforeEach
    void setUp() {
        super.setUp(UpdateConfigSubCommand.class);
    }

    @Singleton
    @Replaces(UpdateConfigurationCall.class)
    public Call<UpdateConfigurationCallInput, String> call() {
        return mockCall;
    }


    @Test
    @DisplayName("--config or --config-path are mandatory options")
    void mandatoryConfigOptions() {
        // When execute without --config or --config-path but with --cluster-url
        commandLine.execute("--cluster-url", "http://localhost:8080");

        // Then
        assertThat(err.toString()).contains(
                "Missing required argument (specify one of these): (--config=<config> | --config-file=<configPath>)"
        );
        // And
        assertThat(out.toString()).isEmpty();
    }

    @Test
    @DisplayName("--cluster-url OR (--host AND --port) are mandatory options")
    void mandatoryConnectivityOptions() {
        // When execute without --cluster-url or --host and --port
        commandLine.execute("--config", "{new: config}");

        // Then
        assertThat(err.toString()).contains(
                "Missing required argument (specify one of these): (--cluster-url=<url> | [--host=<host> --port=<port>]"
        );
        // And
        assertThat(out.toString()).isEmpty();
    }

    @Test
    @DisplayName("--cluster-url and --config are enough to execute")
    void clusterUrlOption() {
        // When
        commandLine.execute("--cluster-url", "http://localhost:8080", "--config", "{new: config}");

        // Then
        assertThat(err.toString()).isEmpty();
        // And
        assertThat(out.toString()).contains("ok");
    }

    @Test
    @DisplayName("--host and --port and --config are enough to execute")
    void hostAndPortOption() {
        // When
        commandLine.execute("--host", "localhost", "--port", "8080", "--config", "{new: config}");

        // Then
        assertThat(err.toString()).isEmpty();
        // And
        assertThat(out.toString()).contains("ok");
    }

    @Test
    @DisplayName("--host requires --port")
    void hostWithoutPortOption() {
        // When
        commandLine.execute("--host", "localhost", "--config", "{new: config}");

        // Then
        assertThat(err.toString()).contains("Missing required argument(s): --port=<port>");
        // And
        assertThat(out.toString()).isEmpty();
    }
}