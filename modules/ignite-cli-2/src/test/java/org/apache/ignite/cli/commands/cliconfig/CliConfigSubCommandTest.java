package org.apache.ignite.cli.commands.cliconfig;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.apache.ignite.cli.commands.decorators.ConfigDecorator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CliConfigSubCommandTest extends CliCommandTestBase {

    @BeforeEach
    void setUp() {
        setUp(CliConfigSubCommand.class);
    }

    @Test
    @DisplayName("Displays all keys")
    void noKey() throws IOException {
        // When executed without arguments
        execute();

        // Then
        String expectedResult = new ConfigDecorator().decorate(TestConfigFactory.createTestConfig()).toTerminalString();
        assertThat(out.toString()).isEqualTo(expectedResult + System.lineSeparator());
        // And
        assertThat(err.toString()).isEmpty();
    }
}