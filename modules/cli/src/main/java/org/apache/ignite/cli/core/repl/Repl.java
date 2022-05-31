package org.apache.ignite.cli.core.repl;

import java.util.Map;
import org.apache.ignite.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.cli.core.repl.prompt.PromptProvider;
import org.apache.ignite.cli.core.repl.terminal.TerminalCustomizer;
import org.jline.reader.Completer;
import org.jline.terminal.Terminal;

/**
 * Data class with all information about REPL.
 */
public class Repl {
    private final PromptProvider promptProvider;
    private final Map<String, String> aliases;
    private final TerminalCustomizer terminalCustomizer;
    private final Class<?> commandClass;
    private final CallExecutionPipelineProvider provider;
    private final Completer completer;

    /**
     * Constructor.
     *
     * @param promptProvider REPL prompt provider.
     * @param commandClass top level command class.
     * @param aliases map of aliases for commands.
     * @param terminalCustomizer customizer of terminal.
     * @param provider default call execution pipeline provider.
     * @param completer completer instance.
     */
    public Repl(PromptProvider promptProvider,
            Class<?> commandClass,
            Map<String, String> aliases,
            TerminalCustomizer terminalCustomizer,
            CallExecutionPipelineProvider provider,
            Completer completer) {
        this.promptProvider = promptProvider;
        this.commandClass = commandClass;
        this.aliases = aliases;
        this.terminalCustomizer = terminalCustomizer;
        this.provider = provider;
        this.completer = completer;
    }

    /**
     * Builder provider method.
     *
     * @return new instance of builder {@link ReplBuilder}.
     */
    public static ReplBuilder builder() {
        return new ReplBuilder();
    }

    public PromptProvider getPromptProvider() {
        return promptProvider;
    }

    /**
     * Getter for {@code commandClass} field.
     *
     * @return class with top level command.
     */
    public Class<?> commandClass() {
        return this.commandClass;
    }

    /**
     * Getter for {@code aliases} field.
     *
     * @return map of command aliases.
     */
    public Map<String, String> getAliases() {
        return aliases;
    }

    /**
     * Method for {@param terminal} customization.
     */
    public void customizeTerminal(Terminal terminal) {
        terminalCustomizer.customize(terminal);
    }

    public CallExecutionPipeline<?, ?> getPipeline(RegistryCommandExecutor executor, String line) {
        return provider.get(executor, line);
    }

    public Completer getCompleter() {
        return completer;
    }
}
