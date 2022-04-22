package org.apache.ignite.cli.call.configuration;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;

/**
 * Updates configuration for node or ignite cluster.
 */
@Singleton
public class UpdateConfigurationCall implements Call<UpdateConfigurationCallInput, String> {
    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput execute(UpdateConfigurationCallInput updateConfigurationCallInput) {
        return DefaultCallOutput.success(
                "Update " + updateConfigurationCallInput.getClusterUrl() + "\n"
                    + updateConfigurationCallInput.getConfig()
        ); //todo: implement
    }
}
