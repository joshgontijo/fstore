package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.network.ClusterMessage;

public class Get implements ClusterMessage {

    public final String streamName;

    public Get(StreamName streamName) {
        this.streamName = streamName.toString();
    }
}
