package org.apache.ignite.cli.commands.configuration;


import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConfigCommandTest extends IntegrationTestBase {

    @Test
    @DisplayName("Should read config when valid cluster-url is given")
    void readDefaultConfig() {
        // When read cluster config with valid url
        execute("config", "read", "--cluster-url", CLUSTER_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Should update config when valid cluster-url is given")
    void updateWholeConfig() {
        // When update the whole cluster configuration
        execute("config", "update", "--cluster-url", CLUSTER_URL, "{root: {new-config-key: new-config-value}}");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // When read the updated cluster configuration
        execute("config", "read", "--cluster-url", CLUSTER_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("{root: {new-config-key: new-config-value}}")
        );
    }

    @Test
    @DisplayName("Should update config with specified path when valid cluster-url is given")
    void updateConfigWithSpecifiedPath() {
        // When update the whole cluster configuration
        execute("config", "update", "--cluster-url", CLUSTER_URL, "root.new-config-key=updated-config-value");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // When read the updated cluster configuration
        execute("config", "read", "--cluster-url", CLUSTER_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("updated-config-value")
        );
    }
}