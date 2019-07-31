package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.network.ClusterMessage;

public class Get implements ClusterMessage {

    public final String streamName;

    public Get(EventId eventId) {
        this.streamName = eventId.toString();
    }
}
