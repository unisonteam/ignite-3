package org.apache.ignite.cli.call.connect;

import org.apache.ignite.cli.core.call.CallInput;

public class ConnectCallInput implements CallInput {
    private final String nodeUrl;

    ConnectCallInput(String nodeUrl) {
        this.nodeUrl = nodeUrl;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public static ConnectCallInputBuilder builder() {
        return new ConnectCallInputBuilder();
    }

    public static class ConnectCallInputBuilder {
        private String nodeUrl;

        public String getNodeUrl() {
            return nodeUrl;
        }

        public ConnectCallInputBuilder nodeUrl(String nodeUrl) {
            this.nodeUrl = nodeUrl;
            return this;
        }

        public ConnectCallInput build() {
            return new ConnectCallInput(nodeUrl);
        }
    }
}
