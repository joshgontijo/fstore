package io.joshworks.eventry.server.cluster.messages;

public class NodeInfoRequested implements ClusterMessage {

    public final String nodeId;

    public NodeInfoRequested(String nodeId) {
        this.nodeId = nodeId;
    }

}
