package io.joshworks.eventry.server.cluster.events;

import io.joshworks.eventry.network.ClusterMessage;

public class PartitionForkCompleted implements ClusterMessage {

    public final int partitionId;

    public PartitionForkCompleted(int partitionId) {
        this.partitionId = partitionId;
    }

}