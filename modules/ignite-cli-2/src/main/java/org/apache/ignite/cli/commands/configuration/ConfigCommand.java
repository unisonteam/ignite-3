package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;

@Command(name = "config", subcommands = {
        ReadConfigSubCommand.class,
        UpdateConfigSubCommand.class
})
@Singleton
public class ConfigCommand implements Runnable {
    @Override
    public void run() {
        // no-op
    }
}
