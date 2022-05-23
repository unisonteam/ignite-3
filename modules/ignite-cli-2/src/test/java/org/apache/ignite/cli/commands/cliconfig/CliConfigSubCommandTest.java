package org.apache.ignite.cli.commands.cliconfig;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.ignite.cli.commands.CliCommandTestBase;
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
    void noKey() {
        // When executed without arguments
        execute();

        // Then
        assertThat(out.toString()).isEqualTo(TestConfigFactory.createTestConfig().printConfig() + System.lineSeparator());
        // And
        assertThat(err.toString()).isEmpty();
    }
}