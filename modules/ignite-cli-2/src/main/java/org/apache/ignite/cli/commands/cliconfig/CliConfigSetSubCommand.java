package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Inject;
import java.util.Map;
import org.apache.ignite.cli.call.cliconfig.CliConfigSetCall;
import org.apache.ignite.cli.call.cliconfig.CliConfigSetCallInput;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Command to set CLI configuration parameters.
 */
@Command(name = "set")
public class CliConfigSetSubCommand implements Runnable {
    @Parameters
    private Map<String, String> parameters;

    @Spec
    private CommandSpec spec;

    @Inject
    private CliConfigSetCall call;

    @Override
    public void run() {
        CallExecutionPipeline.builder(call)
                .inputProvider(() -> new CliConfigSetCallInput(parameters))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
