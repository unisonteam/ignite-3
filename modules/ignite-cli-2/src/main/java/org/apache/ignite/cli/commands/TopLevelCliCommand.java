package org.apache.ignite.cli.commands;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.commands.cliconfig.CliCommand;
import org.apache.ignite.cli.commands.configuration.ConfigCommand;
import org.apache.ignite.cli.commands.sql.SqlCommand;
import org.apache.ignite.cli.commands.status.StatusCommand;
import org.apache.ignite.cli.commands.topology.TopologyCommand;
import org.apache.ignite.cli.commands.version.VersionCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Top-level command that prints help and declares subcommands.
 */
@Command(name = "",
        description = {
                "Welcome to IGnite Shell alpha.",
                "Use @|bold,fg(81) <TAB>|@ to see available commands.",
                "Run @|bold,red ignite|@ to enter the shell.",
                ""},
        subcommands = {
                SqlCommand.class,
                CommandLine.HelpCommand.class,
                ConfigCommand.class,
                VersionCommand.class,
                StatusCommand.class,
                TopologyCommand.class,
                CliCommand.class,
        })
@Singleton
public class TopLevelCliCommand implements Runnable {
    @Override
    public void run() {

    }
}
