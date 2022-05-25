package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;

/**
 * Parent command for CLI configuration commands.
 */
@Command(name = "cli",
        description = "CLI specific commands",
        subcommands = {
                CliConfigSubCommand.class
        })
@Singleton
public class CliCommand {
}
