package org.apache.ignite.cli.commands.version;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItVersionCommandTest extends IntegrationTestBase {

    @Test
    @DisplayName("Should print cli version that is got from pom.xml")
    void printVersion() {
        // When
        execute("version");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Apache Ignite CLI version: ")
        );
    }
}