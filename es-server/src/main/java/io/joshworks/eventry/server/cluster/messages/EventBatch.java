package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.network.ClusterMessage;

import java.util.ArrayList;
import java.util.List;

public class EventBatch implements ClusterMessage {

    public final List<EventRecord> records;

    public EventBatch() {
        this(new ArrayList<>());
    }

    public EventBatch(List<EventRecord> records) {
        this.records = records;
    }
}
