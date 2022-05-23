package org.apache.ignite.cli.call.cliconfig;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.StringCallInput;

/**
 * Gets CLI configuration parameter.
 */
@Singleton
public class CliConfigGetCall implements Call<StringCallInput, String> {
    @Inject
    private Config config;

    @Override
    public DefaultCallOutput<String> execute(StringCallInput input) {
        String key = input.getString();
        if (key != null) {
            String property = config.getProperty(key);
            if (property != null) {
                return DefaultCallOutput.success(property);
            } else {
                return DefaultCallOutput.failure(new IllegalArgumentException("Property " + key + " is not defined"));
            }
        } else {
            return DefaultCallOutput.success(config.printConfig());
        }
    }
}
