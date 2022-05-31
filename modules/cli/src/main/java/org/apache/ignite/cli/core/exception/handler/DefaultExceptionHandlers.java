package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionHandlers;

/**
 * Default collection of exception handlers.
 */
public class DefaultExceptionHandlers extends ExceptionHandlers {

    /**
     * Constructor.
     */
    public DefaultExceptionHandlers() {
        addExceptionHandler(new SqlExceptionHandler());
        addExceptionHandler(new CommandExecutionExceptionHandler());
        addExceptionHandler(new TimeoutExceptionHandler());
        addExceptionHandler(new IgniteClientExceptionHandler());
    }
}
