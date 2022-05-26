package org.apache.ignite.cli.core.repl;

import jakarta.inject.Singleton;

@Singleton
public class Session {
    private boolean connectedToNode;
    private String nodeUrl;


    public boolean isConnectedToNode() {
        return connectedToNode;
    }

    public void setConnectedToNode(boolean connectedToNode) {
        this.connectedToNode = connectedToNode;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public void setNodeUrl(String nodeUrl) {
        this.nodeUrl = nodeUrl;
    }
}
