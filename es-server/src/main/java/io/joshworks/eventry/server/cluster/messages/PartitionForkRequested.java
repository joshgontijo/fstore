package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

public class PartitionForkRequested implements ClusterMessage {

    public final int partitionId;

    public PartitionForkRequested(int partitionId) {
        this.partitionId = partitionId;
    }

}