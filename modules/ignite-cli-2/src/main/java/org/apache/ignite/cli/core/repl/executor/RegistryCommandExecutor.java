package org.apache.ignite.cli.core.repl.executor;


import java.util.Map;
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
        this(systemRegistry, picocliCommands, reader, null);
    }

    /**
     * Constructor.
     *
     * @param systemRegistry {@link SystemRegistry} instance.
     * @param picocliCommands {@link PicocliCommands} instance.
     * @param reader {@link LineReader} instance.
     * @param commands non default command description for widget.
     */
    public RegistryCommandExecutor(SystemRegistry systemRegistry,
                                   PicocliCommands picocliCommands,
                                   LineReader reader,
                                   Map<String, CmdDesc> commands) {
        this.systemRegistry = systemRegistry;
        systemRegistry.register("help", picocliCommands);
        createWidget(reader, systemRegistry::commandDescription, commands);
    }

    private static void createWidget(LineReader reader, Function<CmdLine,CmdDesc> descFun, Map<String, CmdDesc> map) {
        TailTipWidgets widgets = null;
        if (map != null) {
            widgets = new TailTipWidgets(reader, map, 5,
                    TailTipWidgets.TipType.COMPLETER);
        } else if (descFun != null) {
            widgets = new TailTipWidgets(reader, descFun, 5,
                    TailTipWidgets.TipType.COMPLETER);
        }

        if (widgets != null) {
            widgets.enable();
        }
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
