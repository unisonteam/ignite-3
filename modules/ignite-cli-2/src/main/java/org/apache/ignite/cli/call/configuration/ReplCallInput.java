package org.apache.ignite.cli.call.configuration;

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for executing repl commands.
 */
public class ReplCallInput implements CallInput {
    /**
     * Command line.
     */
    private final String line;

    public ReplCallInput(String line) {
        this.line = line;
    }

    /**
     * Command line getter.
     *
     * @return Command line.
     */
    public String getLine() {
        return line;
    }
}
