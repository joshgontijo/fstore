package io.joshworks.fstore.es.shared.messages;

import io.joshworks.fstore.es.shared.EventRecord;

import java.util.List;

public class EventsData {

    public List<EventRecord> events;

    public EventsData() {
    }

    public EventsData(List<EventRecord> events) {
        this.events = events;
    }
}
