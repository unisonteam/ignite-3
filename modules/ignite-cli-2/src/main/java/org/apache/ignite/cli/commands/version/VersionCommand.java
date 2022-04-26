package org.apache.ignite.cli.commands.version;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Command that prints CLI version.
 */
@Command(name = "version", description = "Prints CLI version.")
public class VersionCommand implements Runnable {

    @Spec
    private CommandSpec commandSpec;

    /** {@inheritDoc} */
    @Override
    public void run() {
        commandSpec.commandLine().getOut().println("Apache Ignite CLI version: 0.0.1"); //todo: get version from pom.xml
    }
}
