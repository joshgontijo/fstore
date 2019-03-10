package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

public class IteratorClose implements ClusterMessage {

    public final String uuid;

    public IteratorClose(String uuid) {
        this.uuid = uuid;
    }
}
