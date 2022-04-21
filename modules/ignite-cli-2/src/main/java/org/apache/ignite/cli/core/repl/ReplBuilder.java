package org.apache.ignite.cli.core.repl;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cli.core.repl.terminal.TerminalCustomizer;

public class ReplBuilder {
    private final Map<String, String> aliases = new HashMap<>();
    private TerminalCustomizer terminalCustomizer = terminal -> {
    };

    public Repl build() {
        return new Repl(aliases, terminalCustomizer);
    }

    public ReplBuilder withAliases(Map<String, String> aliases) {
        this.aliases.putAll(aliases);
        return this;
    }

    public ReplBuilder withTerminalCustomizer(TerminalCustomizer terminalCustomizer) {
        this.terminalCustomizer = terminalCustomizer;
        return this;
    }
}
