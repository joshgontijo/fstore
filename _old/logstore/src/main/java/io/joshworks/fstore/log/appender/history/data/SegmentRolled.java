package io.joshworks.fstore.log.appender.history.data;

public class SegmentRolled {

    public final String name;
    public final long timestamp;

    public SegmentRolled(String name, long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
    }
}
