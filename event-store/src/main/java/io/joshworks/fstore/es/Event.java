package io.joshworks.fstore.es;

public class Event {

    public final String type;
    public final String data;
    public final long timestamp;

    public Event(String type, String data, long timestamp) {
        this.type = type;
        this.data = data;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" + "type='" + type + '\'' +
                ", data='" + data + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
