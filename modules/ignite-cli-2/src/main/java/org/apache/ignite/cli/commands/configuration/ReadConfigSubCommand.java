package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.configuration.ReadConfigurationCallInput;
import org.apache.ignite.cli.commands.options.ClusterConnectivityOptions;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallExecutionPipeline;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that reads configuration from the cluster.
 */
@Command(name = "read")
public class ReadConfigSubCommand implements Runnable {

    /**
     * Node ID option.
     */
    @Option(names = {"--node"})
    String nodeId;
    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"})
    String selector;
    /**
     * Mandatory cluster connectivity argument group.
     */
    @ArgGroup(exclusive = true, multiplicity = "1")
    ClusterConnectivityOptions connectivityOptions;

    @Spec
    CommandSpec spec;

    @Inject
    Call<ReadConfigurationCallInput, String> call;

    /** {@inheritDoc} */
    @Override
    public void run() {
        DefaultCallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }

    private ReadConfigurationCallInput buildCallInput() {
        return ReadConfigurationCallInput.builder()
                .clusterUrl(connectivityOptions.getUrl())
                .selector(selector)
                .nodeId(nodeId)
                .build();
    }
}
