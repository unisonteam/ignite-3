package org.apache.ignite.cli.core.repl.executor;

import org.jline.console.SystemRegistry;
import org.jline.reader.LineReader;
import picocli.CommandLine.IFactory;
import picocli.shell.jline3.PicocliCommands;

/**
 * Provider of {@link CommandExecutor}.
 */
public interface CommandExecutorProvider {
    /**
     * Provider methods.
     *
     * @param factory {@link IFactory} instance.
     * @param systemRegistry {@link SystemRegistry} instance.
     * @param picocliCommands {@link PicocliCommands} instance.
     * @param reader {@link LineReader} instance.
     * @return new instance of {@link CommandExecutor}.
     */
    CommandExecutor get(IFactory factory, SystemRegistry systemRegistry, PicocliCommands picocliCommands, LineReader reader);
}
