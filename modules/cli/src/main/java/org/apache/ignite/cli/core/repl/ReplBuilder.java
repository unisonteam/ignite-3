package org.apache.ignite.cli.core.repl;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.cli.core.repl.prompt.PromptProvider;
import org.apache.ignite.cli.core.repl.terminal.TerminalCustomizer;
import org.jline.reader.Completer;

/**
 * Builder of {@link Repl}.
 */
public class ReplBuilder {
    private PromptProvider promptProvider;
    private final Map<String, String> aliases = new HashMap<>();
    private Class<?> commandClass;
    private TerminalCustomizer terminalCustomizer = terminal -> {
    };
    private Completer completer;
    private CallExecutionPipelineProvider provider;

    /**
     * Build methods.
     *
     * @return new instance of {@link Repl}.
     */
    public Repl build() {
        return new Repl(promptProvider, commandClass, aliases, terminalCustomizer, provider, completer);
    }

    public ReplBuilder withPromptProvider(PromptProvider promptProvider) {
        this.promptProvider = promptProvider;
        return this;
    }

    /**
     * Builder setter of {@code commandClass} field.
     *
     * @param commandClass class with top level command.
     * @return invoked builder instance {@link ReplBuilder}.
     */
    public ReplBuilder withCommandClass(Class<?> commandClass) {
        this.commandClass = commandClass;
        return this;
    }

    /**
     * Builder setter of {@code aliases} field.
     *
     * @param aliases map of aliases for commands.
     * @return invoked builder instance {@link ReplBuilder}.
     */
    public ReplBuilder withAliases(Map<String, String> aliases) {
        this.aliases.putAll(aliases);
        return this;
    }

    /**
     * Builder setter of {@code terminalCustomizer} field.
     *
     * @param terminalCustomizer customizer of terminal {@link org.jline.terminal.Terminal}.
     * @return invoked builder instance {@link ReplBuilder}.
     */
    public ReplBuilder withTerminalCustomizer(TerminalCustomizer terminalCustomizer) {
        this.terminalCustomizer = terminalCustomizer;
        return this;
    }

    public ReplBuilder withCompleter(Completer completer) {
        this.completer = completer;
        return this;
    }

    public ReplBuilder withCallExecutionPipelineBuilderProvider(CallExecutionPipelineProvider provider) {
        this.provider = provider;
        return this;
    }
}
