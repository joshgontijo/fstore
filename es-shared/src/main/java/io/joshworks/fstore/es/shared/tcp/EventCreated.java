package io.joshworks.fstore.es.shared.tcp;

public class EventCreated extends Message {

    public long timestamp;
    public int version;

    public EventCreated() {
    }

    public EventCreated(long timestamp, int version) {
        this.timestamp = timestamp;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("EventCreated{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", version=").append(version);
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}
