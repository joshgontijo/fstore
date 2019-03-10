package io.joshworks.eventry.server.cluster.messages;

public class IteratorClose implements ClusterMessage {

    public final String uuid;

    public IteratorClose(String uuid) {
        this.uuid = uuid;
    }
}
