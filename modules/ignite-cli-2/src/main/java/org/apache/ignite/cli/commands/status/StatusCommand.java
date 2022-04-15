package org.apache.ignite.cli.commands.status;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "status", description = "Prints status of the cluster.")
@Singleton
public class StatusCommand implements Runnable {

    @Option(names = {"--cluster-url"})
    private String clusterUrl;

    @Spec
    private CommandSpec commandSpec;


    @Override
    public void run() {
        commandSpec.commandLine().getOut().println("TODO: print status");
    }
}
