package org.apache.ignite.cli.core.repl.executor;

import org.apache.ignite.cli.core.repl.Repl;
import org.jline.console.impl.Builtins;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

public interface CommandExecutorProvider {
    CommandExecutor get(Repl repl,
            Terminal terminal,
            PicocliCommandsFactory factory,
            Builtins builtins,
            LineReader reader);
}
