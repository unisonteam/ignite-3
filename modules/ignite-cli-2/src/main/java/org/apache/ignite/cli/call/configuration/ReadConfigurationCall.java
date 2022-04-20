package org.apache.ignite.cli.call.configuration;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;

/**
 * Reads configuration from ignite cluster.
 */
@Singleton
public class ReadConfigurationCall implements Call<ReadConfigurationCallInput, String> {

    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput execute(ReadConfigurationCallInput readConfigurationInput) {
        return DefaultCallOutput.success("Read from " + readConfigurationInput.getClusterUrl()); //todo: implement
    }
}
