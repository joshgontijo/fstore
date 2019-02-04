package io.joshworks.eventry.server.cluster.message;

public class ClusterEvent {

    public final String uuid;

    public ClusterEvent(String uuid) {
        this.uuid = uuid;
    }
}
