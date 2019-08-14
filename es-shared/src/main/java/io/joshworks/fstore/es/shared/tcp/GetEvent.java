package io.joshworks.fstore.es.shared.tcp;

import io.joshworks.fstore.es.shared.EventId;

public class GetEvent extends Message {

    public final EventId eventId;

    public GetEvent(EventId eventId) {
        this.eventId = eventId;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("GetEvent{");
        sb.append("eventId=").append(eventId);
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}
