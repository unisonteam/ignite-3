package org.apache.ignite.cli.core.repl.executor;

import io.micronaut.configuration.picocli.MicronautFactory;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map.Entry;
import java.util.function.Supplier;
import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.commands.TopLevelCliCommand;
import org.apache.ignite.cli.core.call.DefaultCallExecutionPipeline;
import org.apache.ignite.cli.core.repl.Repl;
import org.fusesource.jansi.AnsiConsole;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Executor of {@link Repl}.
 */
public class ReplExecutor {
    private static final String PROMPT = "ignite-cli> ";

    private final Terminal terminal;

    private final PicocliCommandsFactory factory;
    private final Parser parser = new DefaultParser();
    private final Supplier<Path> workDirProvider = () -> Paths.get(System.getProperty("user.dir"));

    /**
     * Constructor.
     *
     * @param micronautFactory command factory instance {@link MicronautFactory}.
     */
    public ReplExecutor(MicronautFactory micronautFactory) throws Exception {
        factory = new PicocliCommandsFactory(micronautFactory);
        terminal = micronautFactory.create(Terminal.class);
        factory.setTerminal(terminal);
    }

    /**
     * Executor method. This is thread blocking method, until REPL stop executing.
     *
     * @param repl data class of executing REPL.
     */
    public void execute(Repl repl) {
        AnsiConsole.systemInstall();
        try {
            repl.customizeTerminal(terminal);

            Builtins builtins = createBuiltins(repl);
            PicocliCommands picocliCommands = createPicocliCommands();
            SystemRegistryImpl registry = createRegistry();
            registry.setCommandRegistries(builtins, picocliCommands);

            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .completer(registry.completer())
                    .parser(parser)
                    .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                    .build();
            builtins.setLineReader(reader);
            RegistryCommandExecutor executor = new RegistryCommandExecutor(registry, picocliCommands, reader);

            // start the shell and process input until the user quits with Ctrl-D
            while (true) {
                try {
                    executor.cleanUp();
                    String line = reader.readLine(PROMPT, null, (MaskingCallback) null, null);
                    DefaultCallExecutionPipeline.builder(executor)
                            .inputProvider(() -> new ReplCallInput(line))
                            .output(new PrintWriter(System.out, true))
                            .errOutput(new PrintWriter(System.err, true))
                            .build()
                            .runPipeline();
                } catch (UserInterruptException e) {
                    // Ignore
                } catch (EndOfFileException e) {
                    return;
                } catch (Exception e) {
                    executor.trace(e);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            AnsiConsole.systemUninstall();
        }
    }

    private Builtins createBuiltins(Repl repl) {
        // set up JLine built-in commands
        Builtins builtins = new Builtins(workDirProvider, null, null);
        for (Entry<String, String> aliases : repl.getAliases().entrySet()) {
            builtins.alias(aliases.getKey(), aliases.getValue());
        }
        return builtins;
    }

    private SystemRegistryImpl createRegistry() {
        return new SystemRegistryImpl(parser, terminal, workDirProvider, null);
    }

    private PicocliCommands createPicocliCommands() {
        CommandLine cmd = new CommandLine(TopLevelCliCommand.class, factory);
        return new PicocliCommands(cmd);
    }
}
