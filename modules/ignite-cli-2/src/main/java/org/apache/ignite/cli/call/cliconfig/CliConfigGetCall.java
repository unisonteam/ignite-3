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
        String property = config.getProperty(input.getString());
        String body = property != null
                ? property
                : "Property " + input.getString() + " is not defined";
        return DefaultCallOutput.success(body);
    }
}
