package org.apache.ignite.cli.core.repl.executor;


import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.ignite.cli.commands.decorators.core.CommandOutput;
import org.apache.ignite.cli.core.repl.Repl;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import org.jline.widget.TailTipWidgets;
import picocli.CommandLine;
import picocli.CommandLine.IFactory;
import picocli.shell.jline3.PicocliCommands;

public class RegistryCommandExecutor implements CommandExecutor {
    private final SystemRegistryImpl systemRegistry;
    
    public RegistryCommandExecutor(Repl repl, Terminal terminal, IFactory factory, Builtins builtins, LineReader reader) {
        Supplier<Path> workDir = repl.getWorkDirProvider();
        systemRegistry = new SystemRegistryImpl(repl.getParser(), terminal, workDir, null);
        CommandLine cmd = new CommandLine(repl.getCommand(), factory);
        PicocliCommands picocliCommands = new PicocliCommands(cmd);
        systemRegistry.setCommandRegistries(builtins, picocliCommands);
        systemRegistry.register("help", picocliCommands);
    
        TailTipWidgets widgets = new TailTipWidgets(reader, systemRegistry::commandDescription, 5,
                TailTipWidgets.TipType.COMPLETER);
        widgets.enable();
    }
    
    @Override
    public CommandOutput execute(String line) throws Exception {
        Object execute = systemRegistry.execute(line);
        return execute::toString;
    }
    
    @Override
    public void cleanUp() {
        systemRegistry.cleanUp();
    }
    
    
    @Override
    public void trace(Exception e) {
        systemRegistry.trace(e);
    }
    
}
