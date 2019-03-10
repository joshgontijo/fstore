package io.joshworks.eventry.server.cluster.messages;

public class NodeLeft implements ClusterMessage {

    public final String nodeId;

    public NodeLeft(String nodeId) {
        this.nodeId = nodeId;
    }


}
