package org.apache.ignite.cli.call.connect;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;


/**
 * Call for connect to Ignite 3 node.
 */
@Singleton
public class ConnectCall implements Call<ConnectCallInput, String> {

    @Inject
    private final Session session;

    public ConnectCall(Session session) {
        this.session = session;
    }

    @Override
    public CallOutput<String> execute(ConnectCallInput input) {
        NodeConfigurationApi api = createApiClient(input);
        try {
            api.getNodeConfiguration();
        } catch (ApiException e) {
            return DefaultCallOutput.failure(e);
        }

        session.setNodeUrl(input.getNodeUrl());
        session.setConnectedToNode(true);
        return DefaultCallOutput.success("connected to " + input.getNodeUrl());
    }

    @NotNull
    private NodeConfigurationApi createApiClient(ConnectCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getNodeUrl());
        return new NodeConfigurationApi(client);
    }
}
