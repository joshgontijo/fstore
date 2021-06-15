package io.joshworks.fstore.es.shared.messages;

import io.joshworks.fstore.es.shared.EventId;

public class GetEvent {

    public EventId eventId;

    public GetEvent() {
    }

    public GetEvent(EventId eventId) {
        this.eventId = eventId;
    }
}
