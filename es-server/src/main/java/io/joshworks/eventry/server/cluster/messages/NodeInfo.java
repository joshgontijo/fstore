package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

import java.util.Set;

public class NodeInfo implements ClusterMessage {

    public final String nodeId;
    public final Set<Integer> partitions;

    public NodeInfo(String nodeId, Set<Integer> partitions) {
        this.nodeId = nodeId;
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "nodeId='" + nodeId + '\'' +
                ", partitions=" + partitions +
                '}';
    }
}
