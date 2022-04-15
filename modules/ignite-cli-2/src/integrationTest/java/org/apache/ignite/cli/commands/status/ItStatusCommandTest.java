package org.apache.ignite.cli.commands.status;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItStatusCommandTest extends IntegrationTestBase {

    @Test
    @DisplayName("Should print status when valid cluster url is given")
    void printStatus() {
        execute("status", "--cluster-url", CLUSTER_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Cluster status:")
        );
    }
}
