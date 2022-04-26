package org.apache.ignite.cli.commands.topology;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.topology.TopologyCallInput;
import org.apache.ignite.cli.commands.options.ClusterConnectivityOptions;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallExecutionPipeline;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Command that prints ignite cluster topology.
 */
@Command(name = "topology", description = "Prints topology information.")
public class TopologyCommand implements Runnable {

    /**
     * Mandatory cluster connectivity argument group.
     */
    @ArgGroup(exclusive = true, multiplicity = "1")
    ClusterConnectivityOptions connectivityOptions;

    @Spec
    CommandSpec spec;

    @Inject
    Call<TopologyCallInput, String> call;

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

    private TopologyCallInput buildCallInput() {
        return TopologyCallInput.builder()
                .clusterUrl(connectivityOptions.getUrl())
                .build();
    }
}
