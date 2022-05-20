package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;

/**
 * Parent command for CLI configuration commands.
 */
@Command(name = "cli", subcommands = {
        CliConfigSubCommand.class
})
@Singleton
public class CliCommand implements Runnable {
    @Override
    public void run() {
    }
}
