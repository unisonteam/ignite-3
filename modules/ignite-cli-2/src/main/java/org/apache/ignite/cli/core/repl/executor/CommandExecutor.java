package org.apache.ignite.cli.core.repl.executor;

import org.apache.ignite.cli.commands.decorators.core.CommandOutput;

/**
 * Command executor interface.
 */
public interface CommandExecutor {
    /**
     * Execute methods.
     *
     * @param line processed command line.
     * @return output of command execution.
     * @throws Exception in any unhandled case of command execution process.
     */
    CommandOutput execute(String line) throws Exception;

    /**
     * Clean up method.
     */
    void cleanUp();

    /**
     * Handler method for any exception.
     *
     * @param e exception from execute process.
     */
    void trace(Exception e);
}
