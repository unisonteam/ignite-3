package org.apache.ignite.cli.core;

import io.micronaut.configuration.picocli.MicronautFactory;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.repl.Repl;
import org.apache.ignite.cli.core.repl.executor.ReplExecutor;
import org.jline.terminal.Terminal;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

@Singleton
public class CliManager {
    private ReplExecutor replExecutor;

    @Inject
    private Terminal terminal;

    public void init(MicronautFactory micronautFactory) {
        replExecutor = new ReplExecutor(new PicocliCommandsFactory(micronautFactory), terminal);
    }

    public void executeRepl(Repl repl) {
        replExecutor.execute(repl);
    }
}
