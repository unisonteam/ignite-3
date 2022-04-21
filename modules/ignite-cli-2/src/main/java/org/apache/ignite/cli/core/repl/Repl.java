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

    /**
     * Constructor.
     *
     * @param aliases map of aliases for commands.
     * @param terminalCustomizer customizer of terminal.
     */
    public Repl(Map<String, String> aliases, TerminalCustomizer terminalCustomizer) {
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
