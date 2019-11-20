package io.joshworks.fstore.server.cluster.messages;

import io.joshworks.fstore.es.shared.EventRecord;

import java.util.ArrayList;
import java.util.List;

public class EventBatch  {

    public final List<EventRecord> records;

    public EventBatch() {
        this(new ArrayList<>());
    }

    public EventBatch(List<EventRecord> records) {
        this.records = records;
    }
}
