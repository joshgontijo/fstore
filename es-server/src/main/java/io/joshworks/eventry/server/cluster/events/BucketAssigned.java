package io.joshworks.eventry.server.cluster.events;

import io.joshworks.eventry.network.ClusterMessage;

import java.util.Set;

public class BucketAssigned implements ClusterMessage {

    public final String partitionId;
    public final Set<Integer> buckets;

    public BucketAssigned(String partitionId, Set<Integer> buckets) {
        this.partitionId = partitionId;
        this.buckets = buckets;
    }
}
