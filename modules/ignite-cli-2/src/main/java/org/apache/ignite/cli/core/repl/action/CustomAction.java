package org.apache.ignite.cli.core.repl.action;

import java.util.concurrent.Callable;
import org.apache.ignite.cli.commands.decorators.core.CommandOutput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Unmatched;

/**
 * Picocli command wrapper for custom command action.
 */
@Command
public class CustomAction implements Callable<CommandOutput> {
    @Unmatched
    private String[] unmatched;

    private final ReplFunc<String, ? extends CommandOutput> func;

    /**
     * Constructor.
     *
     * @param func command function.
     */
    public CustomAction(ReplFunc<String, ? extends CommandOutput> func) {
        this.func = func;
    }

    /**
     * Execute method.
     *
     * @return command result output.
     * @throws Exception in any unhandled case.
     */
    @Override
    public CommandOutput call() throws Exception {
        return func.apply(unmatched[0]);
    }

    /**
     * Functional interface of command action.
     *
     * @param <T> type of incoming data.
     * @param <R> type of outcoming data.
     */
    public interface ReplFunc<T, R> {
        R apply(T t) throws Exception;
    }
}
