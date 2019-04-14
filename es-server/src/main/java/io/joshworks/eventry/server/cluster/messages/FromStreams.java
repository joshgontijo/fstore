package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

import java.util.Set;

public class FromStreams implements ClusterMessage {

    public final Set<String> streams;
    public final boolean ordered;
    public final int batchSize;
    public final int timeout;//seconds

    public FromStreams(Set<String> streams, int timeout, int batchSize, boolean ordered) {
        this.streams = streams;
        this.batchSize = batchSize;
        this.timeout = timeout;
        this.ordered = ordered;
    }
}
