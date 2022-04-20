package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.DefaultCallExecutionPipeline;
import org.apache.ignite.cli.call.configuration.UpdateConfigurationCall;
import org.apache.ignite.cli.call.configuration.UpdateConfigurationCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Command that updates configuration.
 */
@Command(name = "update")
@Singleton
public class UpdateConfigSubCommand implements Runnable {
    /**
     * Node ID option.
     */
    @Option(names = {"--node"})
    private String nodeId;
    /**
     * Mandatory cluster url option.
     */
    @Option(names = {"--cluster-url"})
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
        DefaultCallExecutionPipeline.builder(call)
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
