package org.apache.ignite.cli.call.configuration;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.exception.CommandExecutionException;
import org.apache.ignite.rest.client.api.ClusterConfigurationApi;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;

/**
 * Updates configuration for node or ignite cluster.
 */
@Singleton
public class UpdateConfigurationCall implements Call<UpdateConfigurationCallInput, String> {
    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<String> execute(UpdateConfigurationCallInput updateConfigurationCallInput) {
        ApiClient client = createApiClient(updateConfigurationCallInput);

        try {
            if (updateConfigurationCallInput.getNodeId() != null) {
                return updateNodeConfig(new NodeConfigurationApi(client), updateConfigurationCallInput);
            } else {
                return updateClusterConfig(new ClusterConfigurationApi(client), updateConfigurationCallInput);
            }
        } catch (ApiException e) {
            throw new CommandExecutionException("Update config", "Ignite api return " + e.getCode());
        }
    }

    private DefaultCallOutput<String> updateClusterConfig(ClusterConfigurationApi api, UpdateConfigurationCallInput input)
            throws ApiException {
        api.updateClusterConfiguration(input.getConfig());
        return DefaultCallOutput.success("");
    }

    private DefaultCallOutput<String> updateNodeConfig(NodeConfigurationApi api, UpdateConfigurationCallInput input) {
        return null;
    }

    @NotNull
    private ApiClient createApiClient(UpdateConfigurationCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getClusterUrl());
        return client;
    }
}
