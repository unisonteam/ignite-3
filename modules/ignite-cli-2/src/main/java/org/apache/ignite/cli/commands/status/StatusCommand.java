package org.apache.ignite.cli.commands.status;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that prints status of ignite cluster.
 */
@Command(name = "status", description = "Prints status of the cluster.")
public class StatusCommand implements Runnable {

    /**
     * Mandatory cluster url option.
     */
    @Option(names = {"--cluster-url"})
    String clusterUrl;

    @Spec
    CommandSpec commandSpec;

    /** {@inheritDoc} */
    @Override
    public void run() {
        commandSpec.commandLine().getOut().println("TODO: print status");
    }
}
