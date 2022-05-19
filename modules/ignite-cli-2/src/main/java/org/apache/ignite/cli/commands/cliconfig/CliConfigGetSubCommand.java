package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.cliconfig.CliConfigGetCall;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StringCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Command to get CLI configuration parameters.
 */
@Command(name = "get")
public class CliConfigGetSubCommand implements Runnable {
    @Parameters
    private String key;

    @Spec
    private CommandSpec spec;

    @Inject
    private CliConfigGetCall call;

    @Override
    public void run() {
        CallExecutionPipeline.builder(call)
                .inputProvider(() -> new StringCallInput(key))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
