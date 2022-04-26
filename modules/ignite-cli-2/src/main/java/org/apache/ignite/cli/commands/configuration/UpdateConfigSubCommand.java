package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Inject;
import java.nio.file.Path;
import org.apache.ignite.cli.call.configuration.UpdateConfigurationCallInput;
import org.apache.ignite.cli.commands.options.ClusterConnectivityOptions;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallExecutionPipeline;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that updates configuration.
 */
@Command(name = "update")
public class UpdateConfigSubCommand implements Runnable {
    /**
     * Node ID option.
     */
    @Option(names = {"--node"})
    String nodeId;
    /**
     * Mandatory cluster connectivity argument group.
     */
    @ArgGroup(exclusive = true, multiplicity = "1")
    ClusterConnectivityOptions connectivityOptions;

    @ArgGroup(exclusive = true, multiplicity = "1")
    ConfigOption configOption;
    @Spec
    CommandSpec spec;
    @Inject
    Call<UpdateConfigurationCallInput, String> call;

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
                .clusterUrl(connectivityOptions.getUrl())
                .config(configOption.config)
                .nodeId(nodeId)
                .build();
    }

    static class ConfigOption {
        /**
         * Plain configuration that will be updated.
         */
        @Option(names = {"--config"}, required = true)
        String config;
        /**
         * Path to configuration file.
         */
        @Option(names = {"--config-file"}, required = true)
        Path configPath;
    }
}
