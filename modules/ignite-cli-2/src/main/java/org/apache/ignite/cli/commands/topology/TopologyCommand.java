package org.apache.ignite.cli.commands.topology;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that prints ignite cluster topology.
 */
@Command(name = "topology", description = "Prints topology information.")
@Singleton
public class TopologyCommand implements Runnable {

    /**
     * Mandatory cluster url.
     */
    @Option(names = {"--cluster-url"})
    private String clusterUrl;

    @Spec
    private CommandSpec commandSpec;

    /** {@inheritDoc} */
    @Override
    public void run() {
        commandSpec.commandLine().getOut().println("Topology command is not implemented yet.");
    }
}
