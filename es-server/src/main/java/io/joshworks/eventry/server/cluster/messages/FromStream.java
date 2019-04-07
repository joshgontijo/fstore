package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

public class FromStream implements ClusterMessage {

    public final String streamName;
    public final int batchSize;
    public final int timeout;//seconds


    public FromStream(String streamName, int timeout, int batchSize) {
        this.streamName = streamName;
        this.batchSize = batchSize;
        this.timeout = timeout;
    }
}
