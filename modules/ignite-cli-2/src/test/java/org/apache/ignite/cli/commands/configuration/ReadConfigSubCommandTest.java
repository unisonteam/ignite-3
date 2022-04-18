package org.apache.ignite.cli.commands.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

@MicronautTest
class ReadConfigSubCommandTest {

    @Inject
    ApplicationContext context;

    CommandLine commandLine;
    StringWriter err;
    StringWriter out;

    @BeforeEach
    void setUp() {
        err = new StringWriter();
        out = new StringWriter();
        commandLine = new CommandLine(ReadConfigSubCommand.class, new MicronautFactory(context));
        commandLine.setErr(new PrintWriter(err));
        commandLine.setOut(new PrintWriter(out));
    }

    @Test
    @DisplayName("Cluster-url is mandatory option")
    void mandatoryOptions() {
        // When execute without --cluster-url
        commandLine.execute();

        // Then
        assertThat(err.toString()).contains("Missing required option: '--cluster-url=<clusterUrl>'");
        // And
        assertThat(out.toString()).isEmpty();
    }
}