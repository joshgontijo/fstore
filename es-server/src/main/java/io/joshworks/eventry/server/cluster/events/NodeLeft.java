package io.joshworks.eventry.server.cluster.events;

import io.joshworks.eventry.network.ClusterMessage;

public class NodeLeft implements ClusterMessage {

    public final String nodeId;

    public NodeLeft(String nodeId) {
        this.nodeId = nodeId;
    }


}
