package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "update")
@Singleton
public class UpdateConfigSubCommand implements Runnable {
    @Option(names = {"--node"})
    private String nodeId;
    @Option(names = {"--cluster-url"})
    private String clusterUrl;
    @Parameters(index = "0")
    private String config;

    @Spec
    private CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().getOut().println("todo: {add: configCall}");
    }
}
