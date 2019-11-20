package io.joshworks.fstore.server.cluster.messages;

import io.joshworks.fstore.es.shared.EventId;

public class Get  {

    public final String streamName;

    public Get(EventId eventId) {
        this.streamName = eventId.toString();
    }
}
