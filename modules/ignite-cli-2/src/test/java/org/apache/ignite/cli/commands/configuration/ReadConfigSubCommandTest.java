package org.apache.ignite.cli.commands.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.TestCall;
import org.apache.ignite.cli.call.configuration.ReadConfigurationCall;
import org.apache.ignite.cli.call.configuration.ReadConfigurationCallInput;
import org.apache.ignite.cli.commands.CommandLineBaseTest;
import org.apache.ignite.cli.core.call.Call;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReadConfigSubCommandTest extends CommandLineBaseTest {

    Call<ReadConfigurationCallInput, String> mockCall = new TestCall<>();

    @BeforeEach
    void setUp() {
        super.setUp(ReadConfigSubCommand.class);
    }

    @Singleton
    @Replaces(ReadConfigurationCall.class)
    public Call<ReadConfigurationCallInput, String> call() {
        return mockCall;
    }

    @Test
    @DisplayName("--cluster-url OR (--host AND --port) are mandatory options")
    void mandatoryConnectivityOptions() {
        // When execute without --cluster-url or --host and --port
        commandLine.execute();

        // Then
        assertThat(err.toString()).contains(
                "Missing required argument (specify one of these): (--cluster-url=<url> | [--host=<host> --port=<port>]"
        );
        // And
        assertThat(out.toString()).isEmpty();
    }

    @Test
    @DisplayName("--cluster-url is enough to execute")
    void clusterUrlOption() {
        // When
        commandLine.execute("--cluster-url", "http://localhost:8080");

        // Then
        assertThat(err.toString()).isEmpty();
        // And
        assertThat(out.toString()).contains("ok");
    }

    @Test
    @DisplayName("--host and --port are enough to execute")
    void hostAndPortOption() {
        // When
        commandLine.execute("--host", "localhost", "--port", "8080");

        // Then
        assertThat(err.toString()).isEmpty();
        // And
        assertThat(out.toString()).contains("ok");
    }

    @Test
    @DisplayName("--host requires --port")
    void hostWithoutPortOption() {
        // When
        commandLine.execute("--host", "localhost");

        // Then
        assertThat(err.toString()).contains("Missing required argument(s): --port=<port>");
        // And
        assertThat(out.toString()).isEmpty();
    }
}