package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;

/**
 * Parent command for CLI configuration commands.
 */
@Command(name = "config", subcommands = {
        CliConfigGetSubCommand.class,
        CliConfigSetSubCommand.class
})
@Singleton
public class ConfigSubCommand implements Runnable {
    @Override
    public void run() {
    }
}
