package io.joshworks.eventry.server.cluster.events;

import io.joshworks.eventry.network.ClusterMessage;

import java.util.Set;

public class NodeJoined implements ClusterMessage {

    public final String nodeId;
    public final Set<String> partitions;

    public NodeJoined(String nodeId, Set<String> partitions) {
        this.nodeId = nodeId;
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "NodeJoined{" + "nodeId='" + nodeId + '\'' +
                ", partitions=" + partitions +
                '}';
    }
}
