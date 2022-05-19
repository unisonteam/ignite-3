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
 * Top-level command that just prints help.
 */
@Command(name = "",
        description = {
                "Example interactive shell with completion and autosuggestions. "
                        + "Hit @|magenta <TAB>|@ to see available commands.",
                "Hit @|magenta ALT-S|@ to toggle tailtips.",
                ""},
        footer = {"", "Press Ctrl-D to exit."},
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
