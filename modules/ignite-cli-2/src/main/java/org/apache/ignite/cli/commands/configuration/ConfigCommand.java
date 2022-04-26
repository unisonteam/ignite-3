package org.apache.ignite.cli.commands.configuration;

import picocli.CommandLine.Command;

/**
 * Parent command for configuration commands.
 */
@Command(name = "config", subcommands = {
        ReadConfigSubCommand.class,
        UpdateConfigSubCommand.class
})
public class ConfigCommand implements Runnable {
    @Override
    public void run() {
        // no-op
    }
}
