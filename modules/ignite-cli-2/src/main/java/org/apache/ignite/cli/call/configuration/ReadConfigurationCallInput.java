package org.apache.ignite.cli.call.configuration;

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for {@link ReadConfigurationCall}.
 */
public class ReadConfigurationCallInput implements CallInput {
    /**
     * Node ID.
     */
    private final String nodeId;
    /**
     * Selector for configuration tree.
     */
    private final String selector;
    /**
     * Cluster url.
     */
    private final String clusterUrl;

    private ReadConfigurationCallInput(String nodeId, String selector, String clusterUrl) {
        this.nodeId = nodeId;
        this.selector = selector;
        this.clusterUrl = clusterUrl;
    }

    /**
     * Get node ID.
     *
     * @return Node ID.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Get selector.
     *
     * @return Selector for configuration tree.
     */
    public String getSelector() {
        return selector;
    }

    /**
     * Get cluster URL.
     *
     * @return Cluster URL.
     */
    public String getClusterUrl() {
        return clusterUrl;
    }

    /**
     * Builder for {@link ReadConfigurationCallInput}.
     */
    public static ReadConfigurationCallInputBuilder builder() {
        return new ReadConfigurationCallInputBuilder();
    }

    /**
     * Builder for {@link ReadConfigurationCallInput}.
     */
    public static class ReadConfigurationCallInputBuilder {
        private String nodeId;
        private String selector;
        private String clusterUrl;

        public ReadConfigurationCallInputBuilder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public ReadConfigurationCallInputBuilder selector(String selector) {
            this.selector = selector;
            return this;
        }

        public ReadConfigurationCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public ReadConfigurationCallInput build() {
            return new ReadConfigurationCallInput(nodeId, selector, clusterUrl);
        }
    }
}
