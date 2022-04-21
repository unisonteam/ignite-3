package org.apache.ignite.cli.core.repl.executor;


import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.jline.console.SystemRegistry;
import org.jline.reader.LineReader;
import org.jline.widget.TailTipWidgets;
import picocli.shell.jline3.PicocliCommands;

public class RegistryCommandExecutor implements Call<ReplCallInput, String> {
    private final SystemRegistry systemRegistry;

    public RegistryCommandExecutor(SystemRegistry systemRegistry, PicocliCommands picocliCommands, LineReader reader) {
        this.systemRegistry = systemRegistry;
        systemRegistry.register("help", picocliCommands);

        TailTipWidgets widgets = new TailTipWidgets(reader, systemRegistry::commandDescription, 5,
                TailTipWidgets.TipType.COMPLETER);
        widgets.enable();
    }

    @Override
    public CallOutput<String> execute(ReplCallInput input) {
        try {
            Object execute = systemRegistry.execute(input.getLine());
            return DefaultCallOutput.success(String.valueOf(execute));
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }

    public void cleanUp() {
        systemRegistry.cleanUp();
    }

    public void trace(Exception e) {
        systemRegistry.trace(e);
    }
}
