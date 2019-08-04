package io.joshworks.fstore.es.shared;

public class EventHeader {

    public final String type;
    public final long timestamp;
    public final String stream;
    public final int version;

    public EventHeader(String type, long timestamp, String stream, int version) {
        this.type = type;
        this.timestamp = timestamp;
        this.stream = stream;
        this.version = version;
    }
}
