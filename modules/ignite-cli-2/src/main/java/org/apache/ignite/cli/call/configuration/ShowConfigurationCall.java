package org.apache.ignite.cli.call.configuration;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.rest.client.api.ClusterConfigurationApi;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;

/**
 * Shows configuration from ignite cluster.
 */
@Singleton
public class ShowConfigurationCall implements Call<ShowConfigurationCallInput, String> {

    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<String> execute(ShowConfigurationCallInput readConfigurationInput) {
        ApiClient client = createApiClient(readConfigurationInput);

        if (readConfigurationInput.getNodeId() != null) {
            return readNodeConfig(new NodeConfigurationApi(client), readConfigurationInput);
        } else {
            return readClusterConfig(new ClusterConfigurationApi(client), readConfigurationInput);
        }
    }

    private DefaultCallOutput<String> readNodeConfig(NodeConfigurationApi api, ShowConfigurationCallInput input) {
        try {
            if (input.getSelector() != null) {
                return DefaultCallOutput.success(api.getNodeConfigurationByPath(input.getSelector()));
            }
            return DefaultCallOutput.success(api.getNodeConfiguration());
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }

    private DefaultCallOutput<String> readClusterConfig(ClusterConfigurationApi api, ShowConfigurationCallInput input) {
        try {
            if (input.getSelector() != null) {
                return DefaultCallOutput.success(api.getClusterConfigurationByPath(input.getSelector()));
            }
            return DefaultCallOutput.success(api.getClusterConfiguration());
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private ApiClient createApiClient(ShowConfigurationCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getClusterUrl());
        return client;
    }
}
