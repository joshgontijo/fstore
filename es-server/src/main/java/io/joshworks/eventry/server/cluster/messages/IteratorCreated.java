package io.joshworks.eventry.server.cluster.messages;

public class IteratorCreated implements ClusterMessage {

    public final String iteratorId;

    public IteratorCreated(String iteratorId) {
        this.iteratorId = iteratorId;
    }

}
