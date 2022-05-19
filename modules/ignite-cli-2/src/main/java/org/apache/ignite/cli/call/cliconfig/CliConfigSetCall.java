package org.apache.ignite.cli.call.cliconfig;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map.Entry;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;

/**
 * Sets CLI configuration parameters.
 */
@Singleton
public class CliConfigSetCall implements Call<CliConfigSetCallInput, String> {
    @Inject
    private Config config;

    @Override
    public DefaultCallOutput<String> execute(CliConfigSetCallInput input) {
        for (Entry<String, String> entry : input.getParameters().entrySet()) {
            config.setProperty(entry.getKey(), entry.getValue());
        }
        config.saveConfig();

        return DefaultCallOutput.success("OK");
    }
}
