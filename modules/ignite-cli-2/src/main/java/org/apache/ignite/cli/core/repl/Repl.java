package org.apache.ignite.cli.core.repl;

import java.util.Map;
import org.apache.ignite.cli.core.repl.terminal.TerminalCustomizer;
import org.jline.terminal.Terminal;

/**
 * Data class with all information about REPL.
 */
public class Repl {
    private final Map<String, String> aliases;
    private final TerminalCustomizer terminalCustomizer;
    private final Class<?> commandClass;

    /**
     * Constructor.
     * @param commandClass
     * @param aliases map of aliases for commands.
     * @param terminalCustomizer customizer of terminal.
     */
    public Repl(Class<?> commandClass,
                Map<String, String> aliases,
                TerminalCustomizer terminalCustomizer) {
        this.commandClass = commandClass;
        this.aliases = aliases;
        this.terminalCustomizer = terminalCustomizer;
    }

    /**
     * Builder provider method.
     *
     * @return new instance of builder {@link ReplBuilder}.
     */
    public static ReplBuilder builder() {
        return new ReplBuilder();
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
}
