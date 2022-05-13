package org.apache.ignite.cli.core.repl.executor;


import java.util.function.Function;
import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.jline.console.CmdDesc;
import org.jline.console.CmdLine;
import org.jline.console.SystemRegistry;
import org.jline.reader.LineReader;
import org.jline.widget.TailTipWidgets;
import picocli.shell.jline3.PicocliCommands;

/**
 * Command executor based on {@link SystemRegistry}.
 */
public class RegistryCommandExecutor implements Call<ReplCallInput, String> {
    private final SystemRegistry systemRegistry;

    /**
     * Constructor.
     *
     * @param systemRegistry {@link SystemRegistry} instance.
     * @param picocliCommands {@link PicocliCommands} instance.
     * @param reader {@link LineReader} instance.
     */
    public RegistryCommandExecutor(SystemRegistry systemRegistry,
                                   PicocliCommands picocliCommands,
                                   LineReader reader) {
        this(systemRegistry, picocliCommands, reader, systemRegistry::commandDescription);
    }

    /**
     * Constructor.
     *
     * @param systemRegistry {@link SystemRegistry} instance.
     * @param picocliCommands {@link PicocliCommands} instance.
     * @param reader {@link LineReader} instance.
     * @param descFunc function that returns command description.
     */
    public RegistryCommandExecutor(SystemRegistry systemRegistry,
                                   PicocliCommands picocliCommands,
                                   LineReader reader,
                                   Function<CmdLine, CmdDesc> descFunc) {
        this.systemRegistry = systemRegistry;
        systemRegistry.register("help", picocliCommands);
        createWidget(reader, descFunc);
    }

    private static void createWidget(LineReader reader, Function<CmdLine, CmdDesc> descFun) {
        if (descFun == null) {
            return;
        }

        TailTipWidgets widgets = new TailTipWidgets(reader, descFun, 5,
                TailTipWidgets.TipType.COMPLETER);
        widgets.enable();
    }

    /**
     * Executor method.
     *
     * @param input processed command line.
     * @return Command output.
     */
    @Override
    public CallOutput<String> execute(ReplCallInput input) {
        try {
            Object execute = systemRegistry.execute(input.getLine());
            return DefaultCallOutput.success(String.valueOf(execute));
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }

    /**
     * Clean up {@link SystemRegistry}.
     */
    public void cleanUp() {
        systemRegistry.cleanUp();
    }

    /**
     * Trace exception.
     *
     * @param e exception instance.
     */
    public void trace(Exception e) {
        systemRegistry.trace(e);
    }
}
