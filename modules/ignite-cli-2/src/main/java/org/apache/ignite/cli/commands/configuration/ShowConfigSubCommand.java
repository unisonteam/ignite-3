package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.configuration.ShowConfigurationCall;
import org.apache.ignite.cli.call.configuration.ShowConfigurationCallInput;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that shows configuration from the cluster.
 */
@Command(name = "show",
description = "Shows configuration.")
public class ShowConfigSubCommand implements Runnable {

    /**
     * Node ID option.
     */
    @Option(names = {"--node"}, description = "Node ID to get local configuration.")
    private String nodeId;
    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"}, description = "Configuration path selector.")
    private String selector;
    /**
     * Mandatory cluster url option.
     */
    @Option(names = {"--cluster-url"}, descriptionKey = "ignite.cluster-url", description = "Url to cluster node.")
    private String clusterUrl = "http://localhost:10300";

    @Spec
    private CommandSpec spec;

    @Inject
    private ShowConfigurationCall call;

    /** {@inheritDoc} */
    @Override
    public void run() {
        CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }

    private ShowConfigurationCallInput buildCallInput() {
        return ShowConfigurationCallInput.builder()
                .clusterUrl(clusterUrl)
                .selector(selector)
                .nodeId(nodeId)
                .build();
    }
}
