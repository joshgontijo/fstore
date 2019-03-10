package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

public class NodeInfoRequested implements ClusterMessage {

    public final String nodeId;

    public NodeInfoRequested(String nodeId) {
        this.nodeId = nodeId;
    }

}
