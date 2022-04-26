package org.apache.ignite.cli.call.topology;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;

@Singleton
public class TopologyCall implements Call<TopologyCallInput, String> {
    @Override
    public CallOutput<String> execute(TopologyCallInput input) {
        return DefaultCallOutput.failure(new RuntimeException("Topology call not implemented yet"));
    }
}
