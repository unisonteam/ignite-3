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

    public Repl(Map<String, String> aliases, TerminalCustomizer terminalCustomizer) {
        this.aliases = aliases;
        this.terminalCustomizer = terminalCustomizer;
    }

    public static ReplBuilder builder() {
        return new ReplBuilder();
    }

    public Map<String, String> getAliases() {
        return aliases;
    }

    public void customizeTerminal(Terminal terminal) {
        terminalCustomizer.customize(terminal);
    }
}
