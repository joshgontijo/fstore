package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

public class IteratorNext implements ClusterMessage {

    public final String uuid;

    public IteratorNext(String uuid) {
        this.uuid = uuid;
    }

}
