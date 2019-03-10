package io.joshworks.eventry.server.cluster.messages;

public class IteratorNext implements ClusterMessage {

    public final String uuid;

    public IteratorNext(String uuid) {
        this.uuid = uuid;
    }

}
