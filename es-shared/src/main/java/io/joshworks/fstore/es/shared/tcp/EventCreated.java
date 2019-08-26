package io.joshworks.fstore.es.shared.tcp;

public class EventCreated {

    public long timestamp;
    public int version;

    public EventCreated() {
    }

    public EventCreated(long timestamp, int version) {
        this.timestamp = timestamp;
        this.version = version;
    }
}
