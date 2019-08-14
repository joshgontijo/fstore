package io.joshworks.fstore.es.shared.tcp;

import io.joshworks.fstore.es.shared.EventRecord;

import java.util.List;

public class EventsData extends Message {

    public final List<EventRecord> events;

    public EventsData(List<EventRecord> events) {
        this.events = events;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("EventsData{");
        sb.append("events=").append(events);
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}
