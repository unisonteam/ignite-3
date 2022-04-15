package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "read")
@Singleton
public class ReadConfigSubCommand implements Runnable {
    @Option(names = {"--node"})
    private String nodeId;
    @Option(names = {"--selector"})
    private String selector;
    @Option(names = {"--cluster-url"})
    private String clusterUrl;
    @Spec
    private CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().getOut().println("todo: {add: configCall}");
    }
}
