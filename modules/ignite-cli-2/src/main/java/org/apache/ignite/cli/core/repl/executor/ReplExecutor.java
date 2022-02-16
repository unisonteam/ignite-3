package org.apache.ignite.cli.core.repl.executor;

import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.function.Supplier;
import org.apache.ignite.cli.core.repl.Repl;
import org.fusesource.jansi.AnsiConsole;
import org.jline.console.impl.Builtins;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

public class ReplExecutor {
    private final PicocliCommandsFactory factory;
    
    public ReplExecutor(PicocliCommandsFactory factory) {
        this.factory = factory;
    }
    
    public void execute(Repl repl) {
        AnsiConsole.systemInstall();
        try {
            Supplier<Path> workDir = repl.getWorkDirProvider();
            // set up JLine built-in commands
            Builtins builtins = new Builtins(workDir, null, null);
            for (Entry<String, String> aliases : repl.getAliases().entrySet()) {
                builtins.alias(aliases.getKey(), aliases.getValue());
            }
            
            try (Terminal terminal = TerminalBuilder.terminal()) {
                factory.setTerminal(terminal);
                repl.customizeTerminal(terminal);
                
                LineReader reader = LineReaderBuilder.builder()
                        .terminal(terminal)
                        .completer(repl.completer())
                        .parser(repl.getParser())
                        .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                        .build();
                builtins.setLineReader(reader);
                CommandExecutor commandExecutor = repl.commandExecutorProvider().get(repl, terminal, factory, builtins, reader);
                
                // start the shell and process input until the user quits with Ctrl-D
                String line;
                String name = repl.getName() + "> ";
                while (true) {
                    try {
                        commandExecutor.cleanUp();
                        line = reader.readLine(name, null, (MaskingCallback) null, null);
                        //TODO: Add decorators support
                        System.out.println(commandExecutor.execute(line));
                    } catch (UserInterruptException e) {
                        // Ignore
                    } catch (EndOfFileException e) {
                        return;
                    } catch (Exception e) {
                        commandExecutor.trace(e);
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            AnsiConsole.systemUninstall();
        }
    }
}
