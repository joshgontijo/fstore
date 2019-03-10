package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

public class NodeLeft implements ClusterMessage {

    public final String nodeId;

    public NodeLeft(String nodeId) {
        this.nodeId = nodeId;
    }


}
