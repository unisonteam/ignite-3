package org.apache.ignite.cli.core.repl.executor;

import io.micronaut.configuration.picocli.MicronautFactory;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.cli.core.exception.handler.ReplExceptionHandlers;
import org.apache.ignite.cli.core.repl.Repl;
import org.apache.ignite.cli.core.repl.expander.NoopExpander;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.LineReader.SuggestionType;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.shell.jline3.PicocliCommands;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Executor of {@link Repl}.
 */
@Singleton
public class ReplExecutor {
    private final Parser parser = new DefaultParser();
    private final Supplier<Path> workDirProvider = () -> Paths.get(System.getProperty("user.dir"));
    private final AtomicBoolean interrupted = new AtomicBoolean();
    private final ReplExceptionHandlers exceptionHandlers = new ReplExceptionHandlers(interrupted::set);
    private PicocliCommandsFactory factory;
    private Terminal terminal;

    /**
     * Secondary constructor.
     *
     * @param micronautFactory command factory instance {@link MicronautFactory}.
     */
    public void injectFactory(MicronautFactory micronautFactory) throws Exception {
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
        try {
            repl.customizeTerminal(terminal);

            PicocliCommands picocliCommands = createPicocliCommands(repl);
            SystemRegistryImpl registry = createRegistry();
            registry.setCommandRegistries(picocliCommands);

            LineReader reader = createReader(repl.getCompleter() != null
                    ? repl.getCompleter()
                    : registry.completer());

            RegistryCommandExecutor executor = new RegistryCommandExecutor(registry, picocliCommands, reader);

            while (!interrupted.get()) {
                try {
                    executor.cleanUp();
                    String prompt = Ansi.AUTO.string(repl.getPromptProvider().getPrompt());
                    String line = reader.readLine(prompt, null, (MaskingCallback) null, null);
                    if (line.isEmpty()) {
                        continue;
                    }

                    repl.getPipeline(executor, line).runPipeline();
                } catch (Throwable t) {
                    exceptionHandlers.handleException(System.err::print, t);
                }
            }
        } catch (Throwable t) {
            exceptionHandlers.handleException(System.err::print, t);
        }
    }

    private LineReader createReader(Completer completer) {
        LineReader result = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(completer)
                .parser(parser)
                .expander(new NoopExpander())
                .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                .build();
        result.setAutosuggestion(SuggestionType.COMPLETER);
        return result;
    }

    public SystemRegistryImpl createRegistry() {
        return new SystemRegistryImpl(parser, terminal, workDirProvider, null);
    }

    private PicocliCommands createPicocliCommands(Repl repl) {
        CommandLine cmd = new CommandLine(repl.commandClass(), factory);
        IDefaultValueProvider defaultValueProvider = repl.defaultValueProvider();
        if (defaultValueProvider != null) {
            cmd.setDefaultValueProvider(defaultValueProvider);
        }
        return new PicocliCommands(cmd);
    }
}
