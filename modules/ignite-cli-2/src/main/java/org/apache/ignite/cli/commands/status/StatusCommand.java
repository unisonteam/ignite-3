package org.apache.ignite.cli.commands.status;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that prints status of ignite cluster.
 */
@Command(name = "status", description = "Prints status of the cluster.")
@Singleton
public class StatusCommand implements Runnable {

    /**
     * Cluster url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to cluster node.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String clusterUrl;

    @Spec
    private CommandSpec commandSpec;

    /** {@inheritDoc} */
    @Override
    public void run() {
        commandSpec.commandLine().getOut().println("TODO: print status");
    }
}
