package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.configuration.ReadConfigurationCall;
import org.apache.ignite.cli.call.configuration.ReadConfigurationCallInput;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
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
    private String nodeId;
    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"})
    private String selector;
    /**
     * Mandatory cluster url option.
     */
    @Option(names = {"--cluster-url"}, required = true)
    private String clusterUrl;

    @Spec
    private CommandSpec spec;

    @Inject
    private ReadConfigurationCall call;

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

    private ReadConfigurationCallInput buildCallInput() {
        return ReadConfigurationCallInput.builder()
                .clusterUrl(clusterUrl)
                .selector(selector)
                .nodeId(nodeId)
                .build();
    }
}
