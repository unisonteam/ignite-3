package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.cliconfig.CliConfigGetCall;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StringCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Parent command for CLI configuration commands.
 */
@Command(name = "config", subcommands = {
        CliConfigGetSubCommand.class,
        CliConfigSetSubCommand.class
})
@Singleton
public class CliConfigSubCommand implements Runnable {
    @Spec
    private CommandSpec spec;

    @Inject
    private CliConfigGetCall call;

    @Override
    public void run() {
        CallExecutionPipeline.builder(call)
                .inputProvider(() -> new StringCallInput(null)) // force getting all properties
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
