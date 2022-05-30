package org.apache.ignite.cli.core.repl;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cli.core.repl.terminal.TerminalCustomizer;

/**
 * Builder of {@link Repl}.
 */
public class ReplBuilder {
    private final Map<String, String> aliases = new HashMap<>();
    private Class<?> commandClass;
    private TerminalCustomizer terminalCustomizer = terminal -> {};

    /**
     * Build methods.
     *
     * @return new instance of {@link Repl}.
     */
    public Repl build() {
        return new Repl(commandClass, aliases, terminalCustomizer);
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
}
