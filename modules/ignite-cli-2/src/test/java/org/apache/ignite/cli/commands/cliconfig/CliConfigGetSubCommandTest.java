package org.apache.ignite.cli.commands.cliconfig;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CliConfigGetSubCommandTest extends CliCommandTestBase {

    @BeforeEach
    void setUp() {
        setUp(CliConfigGetSubCommand.class);
    }

    @Test
    @DisplayName("Displays all keys by default")
    void noKey() {
        // When executed without arguments
        execute();

        // Then
        assertThat(out.toString()).isEqualTo(
                "ignite.cluster-url=test_cluster_url" + System.lineSeparator()
                + "ignite.jdbc-url=test_jdbc_url" + System.lineSeparator());
        // And
        assertThat(err.toString()).isEmpty();
    }

    @Test
    @DisplayName("Displays value for specified key")
    void singleKey() {
        // When executed with single key
        execute("ignite.cluster-url");

        // Then
        assertThat(out.toString()).isEqualTo("test_cluster_url" + System.lineSeparator());
        // And
        assertThat(err.toString()).isEmpty();
    }

    @Test
    @DisplayName("Displays error for nonexistent key")
    void nonexistentKey() {
        // When executed with nonexistent key
        execute("nonexistentKey");

        // Then
        assertThat(err.toString()).contains("Property nonexistentKey is not defined");
        // And
        assertThat(out.toString()).isEmpty();
    }

    @Test
    @DisplayName("Only one key is allowed")
    void multipleKeys() {
        // When executed with multiple keys
        execute("ignite.cluster-url", "ignite.jdbc-url");

        // Then
        assertThat(err.toString()).contains("Unmatched argument at index 1");
        // And
        assertThat(out.toString()).isEmpty();
    }
}