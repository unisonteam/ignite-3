package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.configuration.UpdateConfigurationCall;
import org.apache.ignite.cli.call.configuration.UpdateConfigurationCallInput;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Command that updates configuration.
 */
@Command(name = "update",
        description = "Updates configuration.")
@Singleton
public class UpdateConfigSubCommand implements Runnable {
    /**
     * Node ID option.
     */
    @Option(names = {"--node"}, description = "Node ID to get local configuration.")
    private String nodeId;

    /**
     * Cluster url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to cluster node.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String clusterUrl;

    /**
     * Configuration that will be updated.
     */
    @Parameters(index = "0")
    private String config;

    @Spec
    private CommandSpec spec;

    @Inject
    UpdateConfigurationCall call;

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

    private UpdateConfigurationCallInput buildCallInput() {
        return UpdateConfigurationCallInput.builder()
                .clusterUrl(clusterUrl)
                .config(config)
                .nodeId(nodeId)
                .build();
    }
}
