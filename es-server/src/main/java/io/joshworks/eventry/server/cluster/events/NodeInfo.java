package io.joshworks.eventry.server.cluster.events;

import io.joshworks.eventry.network.ClusterMessage;

import java.util.Set;

public class NodeInfo implements ClusterMessage {

    public final String address;
    public final String nodeId;
    public final Set<Long> streams;

    public NodeInfo(String nodeId, String address, Set<Long> streams) {
        this.nodeId = nodeId;
        this.address = address;
        this.streams = streams;
    }
}
