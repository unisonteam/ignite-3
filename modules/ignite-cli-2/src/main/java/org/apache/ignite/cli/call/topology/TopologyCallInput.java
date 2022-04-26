package org.apache.ignite.cli.call.topology;

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for {@link TopologyCall}
 */
public class TopologyCallInput implements CallInput {
    /**
     * Cluster url.
     */
    private final String clusterUrl;

    public TopologyCallInput(String clusterUrl) {
        this.clusterUrl = clusterUrl;
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    public static TopologyCallInputBuilder builder() {
        return new TopologyCallInputBuilder();
    }

    public static class TopologyCallInputBuilder {
        private String clusterUrl;

        private TopologyCallInputBuilder() {

        }

        public TopologyCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public TopologyCallInput build() {
            return new TopologyCallInput(clusterUrl);
        }
    }
}
