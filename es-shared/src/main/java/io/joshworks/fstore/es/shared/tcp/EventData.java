package io.joshworks.fstore.es.shared.tcp;

import io.joshworks.fstore.es.shared.EventRecord;

public class EventData {

    public EventRecord record;

    public EventData() {
    }

    public EventData(EventRecord record) {
        this.record = record;
    }

}
