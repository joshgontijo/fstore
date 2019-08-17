package io.joshworks.fstore.es.shared.tcp;

import io.joshworks.fstore.es.shared.EventRecord;

public class EventData extends Message {

    public EventRecord record;

    public EventData() {
    }

    public EventData(EventRecord record) {
        this.record = record;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("EventData{");
        sb.append("record=").append(record);
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}
