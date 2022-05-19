package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;

/**
 * Parent command for CLI configuration commands.
 */
@Command(name = "cli", subcommands = {
        ConfigSubCommand.class
})
@Singleton
public class CliCommand implements Runnable {
    @Override
    public void run() {
    }
}
