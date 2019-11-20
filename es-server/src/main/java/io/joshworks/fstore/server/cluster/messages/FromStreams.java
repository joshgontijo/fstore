package io.joshworks.fstore.server.cluster.messages;

import io.joshworks.fstore.es.shared.EventMap;

public class FromStreams  {

    public final EventMap eventMap;
    public final int batchSize;
    public final int timeout;//seconds

    public FromStreams(EventMap eventMap, int timeout, int batchSize) {
        this.eventMap = eventMap;
        this.batchSize = batchSize;
        this.timeout = timeout;
    }
}
