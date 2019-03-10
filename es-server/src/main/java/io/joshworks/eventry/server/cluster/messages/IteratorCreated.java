package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

public class IteratorCreated implements ClusterMessage {

    public final String iteratorId;

    public IteratorCreated(String iteratorId) {
        this.iteratorId = iteratorId;
    }

}
