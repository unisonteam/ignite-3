package org.apache.ignite.cli.core.repl.executor;


import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
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
     * @param withWidget enable or not tail widget {@link TailTipWidgets}.
     */
    public RegistryCommandExecutor(SystemRegistry systemRegistry,
                                   PicocliCommands picocliCommands,
                                   LineReader reader,
                                   boolean withWidget) {
        this.systemRegistry = systemRegistry;
        systemRegistry.register("help", picocliCommands);

        if (withWidget) {
            TailTipWidgets widgets = new TailTipWidgets(reader, systemRegistry::commandDescription, 5,
                                                        TailTipWidgets.TipType.COMPLETER);
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
