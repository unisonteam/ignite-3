package org.apache.ignite.cli.commands.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.apache.ignite.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.cli.core.repl.prompt.PromptProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectCommandTest extends CliCommandTestIntegrationBase {
    @Inject
    PromptProvider promptProvider;

    @Override
    protected @NotNull Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @Test
    @DisplayName("Should connect to cluster with default url")
    void connectWithDefaultUrl() {
        // Given prompt before connect
        assertThat(promptProvider.getPrompt()).isEqualTo("[disconnected]> ");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("connected to http://localhost:10300")
        );
        // And prompt is changed to connect
        assertThat(promptProvider.getPrompt()).isEqualTo("[http://localhost:10300]> ");
    }

    @Test
    @DisplayName("Should connect to cluster with given url")
    void connectWithGivenUrl() {
        // When connect without parameters
        execute("connect", "http://localhost:10301");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("connected to http://localhost:10301")
        );
    }

    @Test
    @DisplayName("Should not connect to cluster with wrong url")
    void connectWithWrongUrl() {
        // When connect without parameters
        execute("connect", "http://localhost:11111");

        // Then
        assertAll(
                () -> assertErrOutputIs("Can not connect to http://localhost:11111")
        );
        // And prompt is
        assertThat(promptProvider.getPrompt()).isEqualTo("[disconnected]> ");
    }

    @Test
    @DisplayName("Should disconnect after connect")
    void disconnect() {
        // Given connected to cluster
        execute("connect");
        // And prompt is
        assertThat(promptProvider.getPrompt()).isEqualTo("[http://localhost:10300]> ");

        // When disconnect
        execute("disconnect");
        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("disconnected from http://localhost:10300")
        );
        // And prompt is changed
        assertThat(promptProvider.getPrompt()).isEqualTo("[disconnected]> ");
    }
}