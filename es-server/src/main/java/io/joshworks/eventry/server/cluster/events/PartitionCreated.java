package io.joshworks.eventry.server.cluster.events;

import io.joshworks.eventry.network.ClusterMessage;

public class PartitionCreated implements ClusterMessage {

    public final String nodeId;
    public final String partitionId;

    public PartitionCreated(String partitionId, String nodeId) {
        this.partitionId = partitionId;
        this.nodeId = nodeId;
    }


    @Override
    public String toString() {
        return "PartitionCreated{" + "id=" + partitionId +
                ", nodeId='" + nodeId + '\'' +
                '}';
    }
}
