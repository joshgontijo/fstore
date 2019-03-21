package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

public class PartitionAssigned implements ClusterMessage {

    public final int id;
    public final String nodeId;

    public PartitionAssigned(int id, String nodeId) {
        this.id = id;
        this.nodeId = nodeId;
    }


    @Override
    public String toString() {
        return "PartitionAssigned{" + "id=" + id +
                ", nodeId='" + nodeId + '\'' +
                '}';
    }
}
